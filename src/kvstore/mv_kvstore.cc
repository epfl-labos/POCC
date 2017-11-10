/*
 * POCC 
 *
 * Copyright 2017 Operating Systems Laboratory EPFL
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#include "kvstore/mv_kvstore.h"
#include "common/sys_config.h"
#include "common/sys_stats.h"
#include "common/utils.h"
#include "common/sys_logger.h"
#include "messages/op_log_entry.pb.h"
#include "kvstore/log_manager.h"
#include <stdexcept>
#include <assert.h>
#include <iostream>
#include <chrono>
#include <numeric>
#include <iostream>

namespace scc {

    MVKVStore *MVKVStore::Instance() {
        static MVKVStore store;
        return &store;
    }

    MVKVStore::MVKVStore() {
        for (int i = 0; i < SysConfig::NumItemIndexTables; i++) {
            _indexTables.push_back(new ItemIndexTable);
            _indexTableMutexes.push_back(new std::mutex);
        }
    }

    MVKVStore::~MVKVStore() {
        for (int i = 0; i < SysConfig::NumItemIndexTables; i++) {
            delete _indexTables[i];
            delete _indexTableMutexes[i];
        }
    }

    void MVKVStore::SetPartitionInfo(DBPartition p,
                                     int numPartitions,
                                     int numReplicasPerPartition,
                                     int totalNumPreloadedKeys) {
        _partitionId = p.PartitionId;
        _replicaId = p.ReplicaId;
        _numPartitions = numPartitions;
        _numReplicasPerPartition = numReplicasPerPartition;
        _totalNumPreloadedKeys = totalNumPreloadedKeys;
    }

    void MVKVStore::Initialize() {
        epoch = 0;
        _localClock = 0;
        _replicaClock = 0;

        _pendingInvisibleVersions.resize(_numReplicasPerPartition);
        _numCheckedVersions = 0;

        PhysicalTimeSpec pt(0, 0);
        for (int i = 0; i < _numReplicasPerPartition; ++i) {
            _PVV.push_back(pt);
#ifdef PVV_COND_VAR
            _pvv_waitHandle.push_back(new WaitHandle(0)); //
#endif
            _LVV.push_back(0);
        }

#ifdef DEP_VECTORS
        for (int i = 0; i < _numReplicasPerPartition; ++i) {
            _GSV.push_back(PhysicalTimeSpec(0, 0));
        }

#else //Scalar
        _initItemVersionPUT = pt;
        SetGST(pt);

#endif //DEP_VECTORS

#ifdef SPIN_LOCK
        _gst_lock = 0;
        _pvv_lock = 0;
#endif

    }

    void MVKVStore::GetSysClockTime(PhysicalTimeSpec &physicalTime, int64_t &logicalTime) {
        PVV_LOCK(_replicaId);

        physicalTime = Utils::GetCurrentClockTime();
        TO_64(physicalTime, physicalTime);
        logicalTime = _localClock;

        // also update local element of VV
        _PVV[_replicaId] = physicalTime;
        _LVV[_replicaId] = logicalTime;

        PVV_UNLOCK(_replicaId);
    }


    PhysicalTimeSpec MVKVStore::GetLST() {
        PhysicalTimeSpec lst;
#ifndef SIXTY_FOUR_BIT_CLOCK
        PVV_ALL_LOCK();
#endif
        lst = _PVV[0];

        for (unsigned int i = 1; i < _PVV.size(); ++i) {
            lst = MIN(lst, _PVV[i]);
        }
#ifndef SIXTY_FOUR_BIT_CLOCK
        PVV_ALL_UNLOCK();
#endif
        return lst;
    }

    PhysicalTimeSpec MVKVStore::GetAndUpdateLST() {
        assert(false); //Not to be used

        PhysicalTimeSpec lst;
        PVV_ALL_LOCK();
        PhysicalTimeSpec now = Utils::GetCurrentClockTime();
        TO_64(_PVV[_replicaId], now);
        lst = _PVV[0];

        for (unsigned int i = 1; i < _PVV.size(); ++i) {
            lst = MIN(lst, _PVV[i]);
        }

        PVV_ALL_UNLOCK();

        return lst;

    }

    PhysicalTimeSpec MVKVStore::GetAndUpdateLSTIfNeeded(PhysicalTimeSpec &target) {
#ifndef SIXTY_FOUR_BIT_CLOCK
        assert(false);
#endif
        if (target > _PVV[_replicaId]) {
            PhysicalTimeSpec now = Utils::GetCurrentClockTime();
            TO_64(now, now);
            PVV_LOCK(_replicaId);
            if (_PVV[_replicaId] < now)
                _PVV[_replicaId] = now;
            PVV_UNLOCK(_replicaId);
        }

        PhysicalTimeSpec lst = _PVV[0];
        for (unsigned int i = 1; i < _PVV.size(); ++i) {
            lst = MIN(lst, _PVV[i]);
        }
        return lst;
    }

#ifdef DEP_VECTORS

    std::vector<PhysicalTimeSpec> MVKVStore::updateAndGetPVV() {
        std::vector<PhysicalTimeSpec> ret;

        PVV_LOCK(_replicaId);
            PhysicalTimeSpec now = Utils::GetCurrentClockTime();
            TO_64(_PVV[_replicaId], now);
            ret = _PVV;
        PVV_UNLOCK(_replicaId);
        return ret;
    }

     void MVKVStore::updatePVV() {

        PVV_LOCK(_replicaId);
            PhysicalTimeSpec now = Utils::GetCurrentClockTime();
            TO_64(_PVV[_replicaId], now);
        PVV_UNLOCK(_replicaId);

    }

    std::vector<PhysicalTimeSpec> MVKVStore::GetPVV() {
        std::vector<PhysicalTimeSpec> ret;
#ifndef SIXTY_FOUR_BIT_CLOCK
        PVV_ALL_LOCK();
#endif
            ret = _PVV;
#ifndef SIXTY_FOUR_BIT_CLOCK
        PVV_ALL_UNLOCK();
#endif
        return ret;
    }

    PhysicalTimeSpec MVKVStore::GetPVV(int rid) {
        PhysicalTimeSpec ret;
#ifndef SIXTY_FOUR_BIT_CLOCK
        PVV_ALL_LOCK();
#endif
            ret = _PVV[rid];
#ifndef SIXTY_FOUR_BIT_CLOCK
        PVV_ALL_UNLOCK();
#endif
        return ret;
    }


    std::vector<PhysicalTimeSpec> MVKVStore::GetGSV() {
#ifndef SIXTY_FOUR_BIT_CLOCK
        std::lock_guard<std::mutex> lk(_GSTMutex);
#endif
        return _GSV;
    }

#endif

    PhysicalTimeSpec MVKVStore::GetGST() {
#ifndef SIXTY_FOUR_BIT_CLOCK
        std::lock_guard <std::mutex> lk(_GSTMutex);
#endif
        return _GST;
    }

    void MVKVStore::SetGST(PhysicalTimeSpec gst) {
#ifndef SIXTY_FOUR_BIT_CLOCK
        {
            std::lock_guard <std::mutex> lk(_GSTMutex);
            ASSERT(gst >= _GST);
            _GST = gst;
        }
#else
        _GST.Seconds = gst.Seconds;
#endif

#ifdef MEASURE_VISIBILITY_LATENCY
        CheckAndRecordVisibilityLatency();
#endif
    }

    void MVKVStore::SetGSTIfSmaller(PhysicalTimeSpec gst) {
        //Assumes 64 bit
#ifndef SIXTY_FOUR_BIT_CLOCK
        assert(false);
#endif
        if (_GST < gst) {
            std::lock_guard <std::mutex> lk(_GSTMutex);
            if (_GST < gst) {
                _GST = gst;
                //Only check visibility latency if we indeed change the gst
#ifdef MEASURE_VISIBILITY_LATENCY
                CheckAndRecordVisibilityLatency();
#endif
            }
        }
    }

#ifdef DEP_VECTORS

    void MVKVStore::SetGSVIfSmaller(std::vector<PhysicalTimeSpec> &gsv) {
        //Assumes 64 bit
#ifndef SIXTY_FOUR_BIT_CLOCK
        assert(false);
#endif
        bool smaller = false;
        int i;
        for (i = 0; i < gsv.size(); i++) {
            if (_GSV[i] < gsv[i]) {
                smaller = true;
                break;
            }
        }
        if (smaller) {

            int j;
            {
                std::lock_guard<std::mutex> lk(_GSTMutex);
                for (j = i; j < gsv.size(); j++) {
                    if (_GSV[j] < gsv[j]) {
                        _GSV[j] = gsv[j];
                    }
                }
            }

#ifdef MEASURE_VISIBILITY_LATENCY
            CheckAndRecordVisibilityLatency();
#endif
        }
    }

    void MVKVStore::SetGSV(std::vector<PhysicalTimeSpec> gsv) {
        std::lock_guard<std::mutex> lk(_GSTMutex);
        for (int i = 0; i < gsv.size(); i++) {
            if (_GSV[i] > gsv[i]) {
                ASSERT(false);
            }
            _GSV[i] = gsv[i];
        }
    }

#endif //DEP_VECTORS

    // only used for initialization (key loading)

    bool MVKVStore::Add(const std::string &key, const std::string &value) {
        int ti = Utils::strhash(key) % SysConfig::NumItemIndexTables;
        if (ti < 0) ti += SysConfig::NumItemIndexTables;

        // check if key exists
        bool keyExisting = false;
        {
            std::lock_guard <std::mutex> lk(*(_indexTableMutexes[ti]));
            if (_indexTables[ti]->find(key) != _indexTables[ti]->end()) {
                keyExisting = true;
            }
        }

        if (!keyExisting) {
            // create new item anchor and version
            ItemAnchor *newAnchor = new ItemAnchor(key, _numReplicasPerPartition);
            ItemVersion *newVersion = new ItemVersion(value);

            newVersion->LUT = 0;
            newVersion->RIT = Utils::GetCurrentClockTime();
            newVersion->PUT = _initItemVersionPUT;
            newVersion->SrcReplica = 0;

#ifdef LOCALITY
            int keyInPartition = std::stoi(key) / _numPartitions;
            int srcReplica = _replicaId;

            assert(srcReplica <=_numReplicasPerPartition);
            newVersion->SrcReplica = srcReplica;
#endif
            newVersion->SrcPartition = _partitionId;
            newVersion->Persisted = true;

#ifdef DEP_VECTORS
            newVersion->DV.resize(_numReplicasPerPartition);
            for (int j = 0; j < _numReplicasPerPartition; j++) {
                newVersion->DV[j] = PhysicalTimeSpec(0, 0);
            }
#endif //DEP_VECTORS
            newAnchor->InsertVersion(newVersion);
            // insert into the index if key still does not exist
            {
                std::lock_guard <std::mutex> lk(*(_indexTableMutexes[ti]));
                if (_indexTables[ti]->find(key) != _indexTables[ti]->end()) {
                    keyExisting = true;

                    // key already exists, release pre-created objects
                    delete newAnchor;
                    delete newVersion;

                } else {
                    _indexTables[ti]->insert(ItemIndexTable::value_type(key, newAnchor));
                }
            }
        }

        // no logging for this operation
#ifdef AVOID_GST
        MVKVStore::AddAgain(key,value);
#endif
        return !keyExisting;
    }

    bool MVKVStore::AddAgain(const std::string &key, const std::string &value) {

        assert(false);
        int ti = Utils::strhash(key) % SysConfig::NumItemIndexTables;
        if (ti < 0) ti += SysConfig::NumItemIndexTables;

        //Treat key as non-existing

        //Retrieve existing anchor
        ItemAnchor *newAnchor = _indexTables[ti]->at(key);
        ItemVersion *newVersion = new ItemVersion(value);

        newVersion->LUT = 0;
        newVersion->RIT = Utils::GetCurrentClockTime();

        PhysicalTimeSpec putTime = _initItemVersionPUT;
        putTime.Seconds += 1;//Newer version w.r.t. previous one
        TO_64(newVersion->PUT, putTime);
        newVersion->SrcReplica = 0;

#ifdef LOCALITY
        int keyInPartition = std::stoi(key) / _numPartitions;
        int srcReplica = ((double) keyInPartition) / 333333.0;
        newVersion->SrcReplica = srcReplica;
#endif

        newVersion->SrcPartition = _partitionId;
        newVersion->Persisted = true;

#ifdef DEP_VECTORS
        newVersion->DV.resize(_numReplicasPerPartition);
        for (int j = 0; j < _numReplicasPerPartition; j++) {
            PhysicalTimeSpec p(1, 0); //With GSV fixed to zero, this would be invisible remotely
            newVersion->DV[j] = p;
        }
#endif
        newAnchor->InsertVersion(newVersion);
        // no logging for this operation
        return true;
    }

#ifdef DEP_VECTORS

    bool MVKVStore::GetOptVec(ConsistencyMetadata &cdata, const std::string &key, std::string &value,
                              ItemAnchor *itemAnchor) {
        ItemVersion *getVersion = itemAnchor->LatestCausalVersion(_replicaId, cdata);
        ASSERT(getVersion != NULL);
        value = getVersion->Value;

#ifndef EMBEDDED_DT_IN_SCRREP_DV
        cdata.DT = getVersion->PUT;
#endif

        cdata.DV = getVersion->DV;
        cdata.SrcReplica = getVersion->SrcReplica;
        return true;
    }
    //This only works with 64 bits clocks
    bool MVKVStore::GetPessVec(ConsistencyMetadata &cdata, const std::string &key, std::string &value,
                               ItemAnchor *itemAnchor) {

#ifdef LOCAL_LINEARIZABLE
        MVKVStore::Instance()->SetGSVIfSmaller(cdata.GSV);
        cdata.GSV = MVKVStore::Instance()->GetGSV();
#else //NON Local Linearizable. Deprecated
        assert(false);
        for (int i = 0; i < _GSV.size(); i++) {
            cdata.GSV[i] = MAX(cdata.GSV[i],_GSV[i]);
        }
#endif //LOCAL_LINEARIZABLE

        ItemVersion *getVersion = itemAnchor->LatestCausalVersion(_replicaId, cdata);
        ASSERT(getVersion != NULL);
        value = getVersion->Value;

#ifndef EMBEDDED_DT_IN_SCRREP_DV
        cdata.DT = getVersion->PUT;
#endif
        cdata.SrcReplica = getVersion->SrcReplica;
        //GSV in cdata already set
        return true;
    }

#else
    bool MVKVStore::GetPessScal(ConsistencyMetadata &cdata, const std::string &key, std::string &value,
                                ItemAnchor *itemAnchor) {
        PhysicalTimeSpec curr_GST;
#ifdef LOCAL_LINEARIZABLE
        MVKVStore::Instance()->SetGSTIfSmaller(cdata.GST);
        curr_GST = MVKVStore::Instance()->GetGST();
#else
        curr_GST = MAX(_GST, cdata.GST); //Lockless in 64 bits clock
#endif
        ItemVersion *getVersion = itemAnchor->LatestCausalVersion(_replicaId, curr_GST);
        ASSERT(getVersion != NULL);
        value = getVersion->Value;
        cdata.DT = getVersion->PUT;
        cdata.GST = curr_GST;
        return true;
    }

#endif

    bool MVKVStore::Get(ConsistencyMetadata &cdata, const std::string &key, std::string &value) {
        int ti = Utils::strhash(key) % SysConfig::NumItemIndexTables;
        if (ti < 0) ti += SysConfig::NumItemIndexTables;

        ItemAnchor *itemAnchor = NULL;

        /** TODO: return the lock */
        {
            std::lock_guard <std::mutex> lk(*(_indexTableMutexes[ti]));
            if (_indexTables[ti]->find(key) == _indexTables[ti]->end()) {
                ASSERT(false);
            } else {
                itemAnchor = _indexTables[ti]->at(key);
            }
        }
#ifndef SIXTY_FOUR_BIT_CLOCK
        assert(false);
#endif

#ifdef DEP_VECTORS
        if (SysConfig::OptimisticMode) {
            return GetOptVec(cdata, key, value, itemAnchor);
        }
        return GetPessVec(cdata, key, value, itemAnchor);
#else
        return GetPessScal(cdata, key, value, itemAnchor);
#endif
    }

    void MVKVStore::WaitOnPut(ConsistencyMetadata &cdata) {

        PhysicalTimeSpec currentTime;
        bool slept = false;
        PhysicalTimeSpec depTime, sleepTime, initSleep;
        int sleep_ms = 0;
        bool blockOnPut = false;

#ifdef DEP_VECTORS
        FROM_64(depTime, cdata.DV[cdata.MaxElId]);
#else
        FROM_64(depTime, cdata.DT);
#endif
        while (depTime > (currentTime = Utils::GetCurrentClockTime())) {

#ifdef MEASURE_STATISTICS
            if (!slept) {
                slept = true;
                initSleep = currentTime;
            }
#endif
            sleepTime = depTime - currentTime;
            int sleep_ms = sleepTime.Seconds * 1000000 + sleepTime.NanoSeconds / 1000 + 1;
            std::this_thread::sleep_for(std::chrono::microseconds(sleep_ms));

            //Busy waiting instead of sleeping if we have to wait for a short amount of time
            if (false) {
                do {
                    __asm volatile ("pause":: : "memory");
                } while (depTime > (currentTime = Utils::GetCurrentClockTime()));
            } else {
                std::this_thread::sleep_for(std::chrono::microseconds(sleep_ms));
            }
        }

#ifdef OPT_BLOCK_ON_PUT
        if (SysConfig::OptimisticMode) {

#ifdef MEASURE_STATISTICS
        // record start time
        PhysicalTimeSpec startTimeOfBlock = Utils::GetCurrentClockTime();
#endif //MEASURE_STATISTICS
           blockOnPut = (MVKVStore::Instance()->waitAlsoOnThePut(cdata));

#ifdef MEASURE_STATISTICS
            if (SysConfig::OptimisticMode) {
                if (blockOnPut) {
                    // record end time
                    PhysicalTimeSpec endTimeOfBlock = Utils::GetCurrentClockTime();
                    double duration = (endTimeOfBlock - startTimeOfBlock).toMilliSeconds();
                    std::lock_guard<std::mutex> lk(SysStats::BlockDurationMutex);
                    SysStats::BlockDuration.push_back(duration);
                    SysStats::NumBlocks += 1;
                }
         }
#endif //MEASURE_STATISTICS
     }
#endif  //OPT_BLOCK_ON_PUT

#ifdef MEASURE_STATISTICS
        if (slept) {
            SysStats::NumDelayedLocalUpdates += 1;
            SysStats::SumDelayedLocalUpdates += (currentTime - initSleep).toMilliSeconds();
        }
#endif //MEASURE_STATISTICS
    }

    bool MVKVStore::Set(ConsistencyMetadata &cdata, const std::string &key, const std::string &value) {
#ifndef FAKE_PUT_WAIT
        WaitOnPut(cdata);
#endif

        int ti = Utils::strhash(key) % SysConfig::NumItemIndexTables;
        if (ti < 0) ti += SysConfig::NumItemIndexTables;

        ItemAnchor *itemAnchor = NULL;

        {
            std::lock_guard <std::mutex> lk(*(_indexTableMutexes[ti]));
            try {
                itemAnchor = _indexTables[ti]->at(key);
            } catch (std::out_of_range &e) {
                if (SysConfig::Debugging) {
                    SLOG((boost::format("MVKVStore: Set key %s does not exist.") % key).str());
                }
                assert(false);
                return false;
            }
        }

        ItemVersion *newVersion = new ItemVersion(value);

        // deleted at Coordinator::SendUpdate()
        LocalUpdate *update = new LocalUpdate();
        update->UpdatedItemAnchor = itemAnchor;
        update->UpdatedItemVersion = newVersion;

        // deleted after update is persisted at local stable storage
        WaitHandle persistedEvent;

        newVersion->SrcPartition = _partitionId;
        newVersion->SrcReplica = _replicaId;
        newVersion->Persisted = false;
        newVersion->Key = key;

#ifdef DEP_VECTORS
        newVersion->DV.resize(cdata.DV.size());
        for (int i = 0; i < cdata.DV.size(); i++) {
            newVersion->DV[i] = cdata.DV[i];
        }
#endif //DEP_VECTORS

//Update of the version vector with the current clock value
        {
            PVV_LOCK(_replicaId);
            PhysicalTimeSpec now = Utils::GetCurrentClockTime();
            newVersion->LUT = _localClock + 1;
            TO_64(newVersion->PUT, now);
            newVersion->RIT = now;

#ifdef EMBEDDED_DT_IN_SCRREP_DV
            newVersion->DV[_replicaId]=newVersion->PUT;
#endif
            itemAnchor->InsertVersion(newVersion);

            // persist operation
            LogManager::Instance()->AppendLog(update, &persistedEvent);
            _localClock += 1;
            _replicaClock += 1;
            _PVV[_replicaId] = newVersion->PUT;
            _LVV[_replicaId] = _localClock;
            PVV_UNLOCK(_replicaId);
        }

        cdata.DT = newVersion->PUT;

        // wait until operation is really logged to stable storage
        persistedEvent.WaitAndReset();
        return true;
    }

#ifdef DEP_VECTORS

    bool MVKVStore::waitOnRead(ConsistencyMetadata &cdata, PhysicalTimeSpec &maxPVV) {
#ifndef SIXTY_FOUR_BIT_CLOCK
        assert(false);
#endif

#ifndef NO_LOCAL_WAIT
        assert(false);
#endif
        unsigned int i;
        bool ret = false;
        const unsigned int size = _PVV.size();

        for (i = 0; i < size; i++) {
            if (i != _replicaId) {
                if (_PVV[i] > maxPVV)
                    maxPVV = _PVV[i];
                if (_PVV[i] < cdata.NDV[i])
                    ret = true;
            }

        }
        return ret;
    }

    bool MVKVStore::waitAlsoOnThePut(ConsistencyMetadata &cdata) {

#ifndef SIXTY_FOUR_BIT_CLOCK
        assert(false);
#endif

#ifndef NO_LOCAL_WAIT
        assert(false);
#endif
        unsigned int i;
        bool ret = false;
        const unsigned int size = _PVV.size();

        for (i = 0; i < size; i++) {
            if (i != _replicaId) {

#ifdef PVV_COND_VAR
                while(GetPVV(i) < cdata.DV[i]){
                        _pvv_waitHandle[i]->SetCountToOneAndWait();
                        ret = true;
                }

#else

#ifdef PVV_VARIABLE_GET
                while (GetPVV(i) < cdata.DV[i]){
#else
                while (_PVV[i] < cdata.DV[i]) {
#endif //PVV_VARIABLE_GET

                    std::this_thread::sleep_for(std::chrono::microseconds(SysConfig::GetSpinTime));
                    ret = true;
                }

#endif //PVV_COND_VAR
            }
        }
        return    ret;
    }

    bool MVKVStore::waitOnReadOnly(ConsistencyMetadata &cdata) {

#ifndef SIXTY_FOUR_BIT_CLOCK
        assert(false);
#endif

#ifndef NO_LOCAL_WAIT
        assert(false);
#endif
        unsigned int i;
        bool ret = false;
        const unsigned int size = _PVV.size();

        for (i = 0; i < size; i++) {
            if (i != _replicaId) {

#ifdef PVV_COND_VAR
                while(GetPVV(i) < cdata.NDV[i]){
                        _pvv_waitHandle[i]->SetCountToOneAndWait();
                        ret = true;
                }

#else //PVV_COND_VAR

#ifdef PVV_VARIABLE_GET
                while (GetPVV(i) < cdata.NDV[i]){
#else
                while (_PVV[i] < cdata.NDV[i]) {
#endif //PVV_VARIABLE_GET

                    std::this_thread::sleep_for(std::chrono::microseconds(SysConfig::GetSpinTime));
                    ret = true;
                }

#endif //PVV_COND_VAR
            }
        }

        return ret;
    }

    bool MVKVStore::waitOnTxOnly(std::vector<PhysicalTimeSpec> &TV) {

#ifndef SIXTY_FOUR_BIT_CLOCK
        assert(false);
#endif

#ifndef NO_LOCAL_WAIT
        assert(false);
#endif
        unsigned int i;
        bool ret = false;
        const unsigned int size = _PVV.size();

        for (i = 0; i < size; i++) {
            if (i != _replicaId) {

#ifdef PVV_COND_VAR
                while(GetPVV(i) < TV[i]){
                        _pvv_waitHandle[i]->SetCountToOneAndWait();
                        ret = true;
                }
#else

#ifdef PVV_VARIABLE_GET
                while (GetPVV(i) < TV[i]){
#else
                while (_PVV[i] < TV[i]) {
#endif //PVV_VARIABLE_GET

                    std::this_thread::sleep_for(std::chrono::microseconds(SysConfig::GetSpinTime));
                    ret = true;
                }

#endif //PVV_COND_VAR
            } else {
                updatePVV();
            }
        }

        return  ret;
    }

#endif

    bool MVKVStore::TxSliceGet(ConsistencyMetadata &cdata,
                               const std::string &key,
                               std::string &value) {

        int ti = Utils::strhash(key) % SysConfig::NumItemIndexTables;
        if (ti < 0) ti += SysConfig::NumItemIndexTables;

        ItemAnchor *itemAnchor = NULL;

        {
            std::lock_guard <std::mutex> lk(*(_indexTableMutexes[ti]));
            if (_indexTables[ti]->find(key) == _indexTables[ti]->end()) {
                fprintf(stderr, "Asking for non existing key %s\n", key.c_str());

                assert(false);
            } else {
                itemAnchor = _indexTables[ti]->at(key);
            }
        }

        // read version
#ifndef DEP_VECTORS  //SCALAR
#ifdef LOCAL_LINEARIZABLE
        SetGSTIfSmaller(cdata.GST);
#endif
        ItemVersion *getVersion = itemAnchor->LatestSnapshotVersion(cdata.GST);
        ASSERT(getVersion != NULL);
        value = getVersion->Value;
        cdata.DT = getVersion->PUT;

#else  //VECTOR
        if (!SysConfig::OptimisticMode) {
            //We do not necessarily set the GSV on the server.
#ifdef LOCAL_LINEARIZABLE
            SetGSVIfSmaller(cdata.GSV);
#endif //LOCAL_LINEAR
            cdata.GSV[_replicaId] = cdata.DT;
        } else {
            //In the OPTIMISTIC mode, the cdata.GSV is the TRANSACTION VECTOR
            waitOnTxOnly(cdata.GSV);
        }

        ItemVersion *getVersion = itemAnchor->LatestSnapshotVersion(cdata.GSV, _replicaId);
        ASSERT(getVersion != NULL);

        //Results to be returned
        value = getVersion->Value;
        cdata.DT = getVersion->PUT;
        cdata.SrcReplica = getVersion->SrcReplica;
        //We do not set return GSV on the server. The xact upper bound has been determined on the coordinator node
#endif
        return true;
    }

#ifdef PARALLEL_XACTS

    bool MVKVStore::LocalTxSliceGet(ConsistencyMetadata &cdata, const std::string &key, ParallelXact &p_x, int index) {

        int ti = Utils::strhash(key) % SysConfig::NumItemIndexTables;
        if (ti < 0) ti += SysConfig::NumItemIndexTables;

        ItemAnchor *itemAnchor = NULL;

        {
            std::lock_guard<std::mutex> lk(*(_indexTableMutexes[ti]));
            if (_indexTables[ti]->find(key) == _indexTables[ti]->end()) {
                assert(false);
            } else {
                itemAnchor = _indexTables[ti]->at(key);
            }
        }

        // read version
#ifndef DEP_VECTORS  //SCALAR
#ifdef LOCAL_LINEARIZABLE
        SetGSTIfSmaller(cdata.GST);
#endif
        ItemVersion *getVersion = itemAnchor->LatestSnapshotVersion(cdata.GST);


#else  //VECTOR
        if (!SysConfig::OptimisticMode) {
            //We do not necessarily set the GSV on the server.
#ifdef LOCAL_LINEARIZABLE
            SetGSVIfSmaller(cdata.GSV);
#endif
        }

#ifdef EMBEDDED_DT_IN_SCRREP_DV
        cdata.GSV[_replicaId]  = cdata.DT;
#endif
        ItemVersion *getVersion = itemAnchor->LatestSnapshotVersion(cdata.GSV, _replicaId);
#endif //VECTOR

        ASSERT(getVersion != NULL);
        STORE_RESULTS(p_x, index, getVersion->Value, getVersion->PUT, getVersion->SrcReplica);
        return true;
    }
#endif //PARALLEL_XACT

    bool MVKVStore::OptTxSliceGetOld(ConsistencyMetadata &cdata,
                                     const std::string &key,
                                     std::string &value) {
#ifdef MEASURE_STATISTICS
        bool slept = false;
        PhysicalTimeSpec initWait;
#endif

        int ti = Utils::strhash(key) % SysConfig::NumItemIndexTables;
        if (ti < 0) ti += SysConfig::NumItemIndexTables;

        ItemAnchor *itemAnchor = NULL;

        {
            std::lock_guard <std::mutex> lk(*(_indexTableMutexes[ti]));
            if (_indexTables[ti]->find(key) == _indexTables[ti]->end()) {
                fprintf(stdout, "Asked key: %s\n", key.c_str());
                assert(false);
            } else {
                itemAnchor = _indexTables[ti]->at(key);
            }
        }

        ASSERT(itemAnchor != NULL);

        // read version
#ifndef DEP_VECTORS  //SCALAR
        PhysicalTimeSpec depTime;
        PhysicalTimeSpec lst, lst2;

        while (cdata.GST > (lst = GetAndUpdateLSTIfNeeded(cdata.GST))) {
#ifdef MEASURE_STATISTICS
            if(!slept){
                    slept = true;
                    initWait = Utils::GetCurrentClockTime();
            }
#endif
            std::this_thread::sleep_for(std::chrono::microseconds(SysConfig::GetSpinTime));
        }


#ifdef MEASURE_STATISTICS
        PhysicalTimeSpec endWait;
         if(slept){
                endWait =  Utils::GetCurrentClockTime();
                }
#endif
        ItemVersion *getVersion = itemAnchor->LatestSnapshotVersionOPT(cdata.GST);
        ASSERT(getVersion != NULL);

        value = getVersion->Value;
        cdata.DT = getVersion->PUT;
#else //VECTOR
        std::vector<PhysicalTimeSpec> pvv = GetPVV();
        for (int i = 0; i < cdata.GSV.size(); i++) {
            while (cdata.GSV[i] > pvv[i]) {
#ifdef MEASURE_STATISTICS
                if (!slept) {
                    slept = true;
                    initWait = Utils::GetCurrentClockTime();
                }
#endif
                std::this_thread::sleep_for(std::chrono::microseconds(SysConfig::GetSpinTime));
                if (i == _replicaId) {
                    pvv = updateAndGetPVV();
                } else {
                    pvv = GetPVV();
                }
            }
        }
#ifdef MEASURE_STATISTICS
        PhysicalTimeSpec endWait;
        if (slept) endWait = Utils::GetCurrentClockTime();
#endif
        ItemVersion *getVersion = itemAnchor->LatestSnapshotVersion(cdata.GSV, _replicaId);
        ASSERT(getVersion != NULL);
        value = getVersion->Value;
        cdata.DT = getVersion->PUT;
        cdata.SrcReplica = getVersion->SrcReplica;
#endif  //DEP_VECTORS

#ifdef MEASURE_STATISTICS
        if (slept) {
            endWait = endWait - initWait;
            cdata.waited_xact = endWait.toMilliSeconds();
        } else {
            cdata.waited_xact = 0;
        }
#endif
        return true;
    }

    bool MVKVStore::ShowItem(const std::string &key, std::string &itemVersions) {
        int ti = Utils::strhash(key) % SysConfig::NumItemIndexTables;
        if (ti < 0) ti += SysConfig::NumItemIndexTables;

        ItemAnchor *itemAnchor = NULL;
        {
            std::lock_guard <std::mutex> lk(*(_indexTableMutexes[ti]));
            if (_indexTables[ti]->find(key) == _indexTables[ti]->end()) {
                return false;
            } else {
                itemAnchor = _indexTables[ti]->at(key);
            }
        }

        itemVersions = itemAnchor->ShowItemVersions();

        return true;
    }

    bool MVKVStore::ShowDB(std::string &allItemVersions) {
        for (int i = 0; i < SysConfig::NumItemIndexTables; i++) {
            std::lock_guard <std::mutex> lk(*(_indexTableMutexes[i]));
            for (ItemIndexTable::iterator it = _indexTables[i]->begin();
                 it != _indexTables[i]->end(); it++) {
                std::string itemVersions = it->second->ShowItemVersions();
                allItemVersions += (itemVersions + "\n");
            }
        }
        return true;
    }

    bool MVKVStore::ShowState(std::string &stateStr) {

        SLOG("Entering ShowState");
        SLOG((boost::format("REPLICA_ID %d") % _replicaId).str());
        SLOG((boost::format("RET_LOCAL %d") % SysStats::NumReturnedLocalItemVersions).str());
        SLOG((boost::format("RET_IMMEDIATELY_LOCAL %d") % SysStats::NumReturnedImmediatelyLocalItemVersions).str());
        SLOG((boost::format("RET_REMOTE %d") % SysStats::NumReturnedRemoteItemVersions).str());
        SLOG((boost::format("RET_LATEST_ADDED %d") % SysStats::NumReturnedLatestAddedItemVersions).str());
        SLOG((boost::format("RET_NOT_LATEST_ADDED %d") % SysStats::NumReturnedNOTLatestAddedItemVersions).str());
        SLOG((boost::format("RET_STALE %d") % SysStats::NumReturnedStaleItemVersions).str());
        SLOG((boost::format("RET_TIMES_UNMERGED %d") % SysStats::NumTimesReturnedUnmergedItemVersion).str());
        SLOG((boost::format("RET_TOTAL %d") % SysStats::NumReturnedItemVersions).str());
        std::vector <PhysicalTimeSpec> pvv;
        std::vector <int64_t> lvv;
        {
            PVV_ALL_LOCK();
            pvv = _PVV;
            lvv = _LVV;
            PVV_ALL_UNLOCK();
        }

        double average_staleness_time = 0;
        double average_staleness_time_total = 0;
        StalenessStatistics stat;
        OptVersionBlockStatistics bStat;
        bStat.type = "block time";
        UnmergedVersionsStatistics umvStat;
        BlockDurationPercentilesStatistics bds;
#ifdef MEASURE_STATISTICS
        if (!SysConfig::OptimisticMode) {
            calculateStalenessStatistics(stat);
            calculateUnmergedVersionsStatistics(umvStat);

        } else {
            calculateStalenessStatistics(stat);
            calculateUnmergedVersionsStatistics(umvStat);
            calculateBlockStatistics(bStat);
            CreateBlockDurationPercentileStats(bds);
            umvStat.value = 0;
        }
#endif

        std::string shortForm = (boost::format(
                "%d|rid=%d|%lu|%ld|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%.5lf|%.5lf|%d|%d|%s|%s|%s|%d|%d|%d|%d|%d|%d|%d|%d|\n")
                                 % _partitionId
                                 % _replicaId
                                 % SysStats::NumPendingPropagatedUpdates
                                 % LogManager::Instance()->NumReplicatedBytes
                                 % SysStats::NumPublicGetRequests
                                 % SysStats::NumPublicSetRequests
                                 % SysStats::NumInternalGetRequests
                                 % SysStats::NumInternalSetRequests
                                 % SysStats::NumDelayedLocalUpdates
                                 % SysStats::NumRecvUpdateReplicationMsgs
                                 % SysStats::NumRecvUpdateReplications
                                 % SysStats::NumRecvGSTs
                                 % SysStats::NumRecvGSTBytes
                                 % SysStats::NumSendInternalLSTBytes
                                 % _numCheckedVersions
                                 % SysStats::NumReturnedItemVersions
                                 % SysStats::NumReturnedStaleItemVersions
                                 % stat.averageNumFresherVersionsInItemChain
                                 % stat.averageUserPerceivedStalenessTime
                                 % _localClock
                                 % _replicaClock
                                 % Utils::physicaltime2str(GetGST())
                                 % Utils::physicaltv2str(pvv)
                                 % Utils::logicaltv2str(lvv)
                                 % SysStats::NumReceivedPropagatedUpdates
                                 % SysStats::NumRecvHeartbeats
                                 % SysStats::NumSentUpdates
                                 % SysStats::NumUpdatesStoredInLocalUpdateQueue
                                 % SysStats::NumUpdatesStoredInToPropagateLocalUpdateQueue
                                 % SysStats::NumUPropagatedPersistedUpdates
                                 % SysStats::NumGSTRounds
                                 % SysStats::NumInitiatedGSTs).str();

        stateStr += shortForm;
        VisibilityStatistics vv;
        ShowVisibilityLatency(vv);

        stateStr += (
                boost::format(
                        "local clock %d," // 0
                                " replica clock %d," // 1
                                " GSV %s,"  // 2
                                " LST %s\n" // 3
                                "physical version vector %s\n" // 4
                                "logical version vector %s\n" // 5
                                "pending propagated updates %lu\n" // 6
                                "num replicated bytes %ld\n" // 7
                                "num public get requests %d\n" // 8
                                "num public set requests %d\n"  // 9
                                "num internal get requests %d\n" // 10
                                "num internal set requests %d\n" //11
                                "num delayed local updates %d\n" //12
                                "num recieved update replication messages %d\n" //13
                                "num recieved update replications %d\n" //14
                                "num received GSTs %d\n" //15
                                "num received GST bytes %d\n" //16
                                "num send internal LST bytes %d\n" //17
                                "num latency checked versions %d\n" //18
                                "num returned item versions %d\n" //19
                                "num returned stale item versions %d\n" //20
                                "average num fresher versions in item chain %.5lf ms\n" //21
                                "average user perceived staleness time %.5lf ms\n" //22
                                "min user perceived staleness time %.5lf ms\n" //23
                                "max user perceived staleness time %.5lf ms\n" //24
                                "median staleness time %.5lf ms \n" //25
                                "90 percentile staleness time %.5lf ms \n" //26
                                "95 percentile staleness time %.5lf ms \n" //27
                                "99 percentile staleness time %.5lf ms \n" //28
                                "num of received propagated updates %d\n" //29
                                "num recv heartbeats %d\n" //30
                                "NumSentUpdates %d\n" //31
                                "NumUpdatesStoredInLocalUpdateQueue %d\n" //32
                                "NumUpdatesStoredInToPropagateLocalUpdateQueue %d\n" //33
                                "NumUPropagatedPersistedUpdates %d\n" //34
                                "NumTimesOPTProtocolBlocks %d\n" //35
                                "NumBlocksCond1 %d\n" //36
                                "NumBlocksCond2 %d\n" //37
                                "%s\n"//38 //38 block staleness statistics
                                "NumLSTCatchup %d\n"//39
                                "NumUselessBlocks %d\n"//40
                                "NumXacts %d\n"//41
                                "NumXactBlocks %d\n"//42
                                "XactsBlockDuration %f\n"//43
                                "sum_get_block %f\n"//44
                                "sum_fresher_versions %f\n" //45
                                "sum_perceived_staleness %f\n" //46
                                "NumSentGSTBytes %d\n" //47
                                "NumRecvInternalLSTBytes %d\n" //48
                                "NumSentUSVBytes %d\n" //49
                                "NumRecvUSVBytes %d\n" //50
                                "NumSentReplicationBytes %d\n" //51
                                "NumRecvReplicationBytes %d\n" //52
                                "ReplicationBatch %d\n" //53
                                "NumSentBatches %d\n" //54
                                "NumReceivedBatches %d\n" //55
                                "SumDelayedLocalUpdates %d\n"
                                "Overall_10 %d\n"
                                "Overall_20 %d\n"
                                "Overall_30 %d\n"
                                "Overall_40 %d\n"
                                "Overall_50 %d\n"
                                "Overall_60 %d\n"
                                "Overall_70 %d\n"
                                "Overall_80 %d\n"
                                "Overall_90 %d\n"
                                "Overall_95 %d\n"
                                "Overall_99 %d\n"
                                "Propagation_10 %d\n"
                                "Propagation_20 %d\n"
                                "Propagation_30 %d\n"
                                "Propagation_40 %d\n"
                                "Propagation_50 %d\n"
                                "Propagation_60 %d\n"
                                "Propagation_70 %d\n"
                                "Propagation_80 %d\n"
                                "Propagation_90 %d\n"
                                "Propagation_95 %d\n"
                                "Propagation_99 %d\n"
                                "Visibility_10 %d\n"
                                "Visibility_20 %d\n"
                                "Visibility_30 %d\n"
                                "Visibility_40 %d\n"
                                "Visibility_50 %d\n"
                                "Visibility_60 %d\n"
                                "Visibility_70 %d\n"
                                "Visibility_80 %d\n"
                                "Visibility_90 %d\n"
                                "Visibility_95 %d\n"
                                "Visibility_99 %d\n"
                                "BlockDuration_10 %d\n"
                                "BlockDuration_20 %d\n"
                                "BlockDuration_30 %d\n"
                                "BlockDuration_40 %d\n"
                                "BlockDuration_50 %d\n"
                                "BlockDuration_60 %d\n"
                                "BlockDuration_70 %d\n"
                                "BlockDuration_80 %d\n"
                                "BlockDuration_90 %d\n"
                                "BlockDuration_95 %d\n"
                                "BlockDuration_99 %d\n"
                                "NumTimesReturnedUnmergedItemVersion %d\n"
                                "TotalNumRetUnmergedItemVersions %lf\n"

                )
                % _localClock  //0
                % _replicaClock //1
                % Utils::physicaltv2str(GetGSV()) //2
                % Utils::physicaltime2str(GetLST()) //3
                % Utils::physicaltv2str(pvv) //4
                % Utils::logicaltv2str(lvv) //5
                % SysStats::NumPendingPropagatedUpdates //6
                % LogManager::Instance()->NumReplicatedBytes //7
                % SysStats::NumPublicGetRequests //8
                % SysStats::NumPublicSetRequests //9
                % SysStats::NumInternalGetRequests //10
                % SysStats::NumInternalSetRequests //11
                % SysStats::NumDelayedLocalUpdates //12
                % SysStats::NumRecvUpdateReplicationMsgs //13
                % SysStats::NumRecvUpdateReplications //14
                % SysStats::NumRecvGSTs //15
                % SysStats::NumRecvGSTBytes //16
                % SysStats::NumSendInternalLSTBytes //17
                % _numCheckedVersions //18
                % SysStats::NumReturnedItemVersions //19
                % SysStats::NumReturnedStaleItemVersions //20
                % stat.averageNumFresherVersionsInItemChain //21
                % stat.averageUserPerceivedStalenessTime //22
                % stat.minStalenessTime //23
                % stat.maxStalenessTime //24
                % stat.medianStalenessTime //25
                % stat._90PercentileStalenessTime //26
                % stat._95PercentileStalenessTime //27
                % stat._99PercentileStalenessTime //28
                % SysStats::NumReceivedPropagatedUpdates //29
                % SysStats::NumRecvHeartbeats //30
                % SysStats::NumSentUpdates //31
                % SysStats::NumUpdatesStoredInLocalUpdateQueue //32
                % SysStats::NumUpdatesStoredInToPropagateLocalUpdateQueue //33
                % SysStats::NumUPropagatedPersistedUpdates //34
                % SysStats::NumBlocks //35
                % SysStats::NumBlocksCond1 //36
                % SysStats::NumBlocksCond2 //37
                % bStat.toString() //38
                % SysStats::NumLSTCatchup //39
                % SysStats::NumUselessBlocks //40
                % SysStats::NumXacts
                % SysStats::NumXactBlocks
                % (((double) SysStats::XactBlockDuration) * 0.001)  //usec to msec
                % std::accumulate(SysStats::BlockDuration.begin(), SysStats::BlockDuration.end(), 0)
                % std::accumulate(SysStats::NumFresherVersionsInItemChain.begin(),
                                  SysStats::NumFresherVersionsInItemChain.end(), 0)
                % std::accumulate(SysStats::UserPerceivedStalenessTime.begin(),
                                  SysStats::UserPerceivedStalenessTime.end(), 0)
                % SysStats::NumSentGSTBytes
                % SysStats::NumRecvInternalLSTBytes
                % SysStats::NumSentUSVBytes
                % SysStats::NumRecvUSVBytes
                % SysStats::NumSentReplicationBytes
                % SysStats::NumRecvReplicationBytes
                % SysConfig::UpdatePropagationBatchTime
                % SysStats::NumSentBatches
                % SysStats::NumReceivedBatches
                % SysStats::SumDelayedLocalUpdates
                % vv.overall[0]
                % vv.overall[1]
                % vv.overall[2]
                % vv.overall[3]
                % vv.overall[4]
                % vv.overall[5]
                % vv.overall[6]
                % vv.overall[7]
                % vv.overall[8]
                % vv.overall[9]
                % vv.overall[10]
                % vv.propagation[0]
                % vv.propagation[1]
                % vv.propagation[2]
                % vv.propagation[3]
                % vv.propagation[4]
                % vv.propagation[5]
                % vv.propagation[6]
                % vv.propagation[7]
                % vv.propagation[8]
                % vv.propagation[9]
                % vv.propagation[10]
                % vv.visibility[0]
                % vv.visibility[1]
                % vv.visibility[2]
                % vv.visibility[3]
                % vv.visibility[4]
                % vv.visibility[5]
                % vv.visibility[6]
                % vv.visibility[7]
                % vv.visibility[8]
                % vv.visibility[9]
                % vv.visibility[10]
                % bds.percentiles[0]
                % bds.percentiles[1]
                % bds.percentiles[2]
                % bds.percentiles[3]
                % bds.percentiles[4]
                % bds.percentiles[5]
                % bds.percentiles[6]
                % bds.percentiles[7]
                % bds.percentiles[8]
                % bds.percentiles[9]
                % bds.percentiles[10]
                % SysStats::NumTimesReturnedUnmergedItemVersion
                % umvStat.value

        ).str();

        // addStalenessTimesToOutput(stateStr);

        return true;
    }


    bool MVKVStore::DumpLatencyMeasurement(std::string &resultStr) {
        std::lock_guard <std::mutex> lk(_pendingInvisibleVersionsMutex);

        for (unsigned int i = 0; i < _recordedLatencies.size(); ++i) {

            ReplicationLatencyResult &r = _recordedLatencies[i];
            resultStr +=
                    std::to_string(r.OverallLatency.Seconds * 1000 +
                                   r.OverallLatency.NanoSeconds / 1000000.0) + " " +
                    std::to_string(r.PropagationLatency.Seconds * 1000 +
                                   r.PropagationLatency.NanoSeconds / 1000000.0) + " " +
                    std::to_string(r.VisibilityLatency.Seconds * 1000 +
                                   r.VisibilityLatency.NanoSeconds / 1000000.0) + " [ms] " +
                    std::to_string(r.SrcReplica) + "\n";
        }

        return true;
    }


    void MVKVStore::ShowVisibilityLatency(VisibilityStatistics &v) {

        std::string resultStr = "";
#ifdef MEASURE_VISIBILITY_LATENCY
        std::lock_guard <std::mutex> lk(_pendingInvisibleVersionsMutex);
        std::vector<double> overallLatency;
        std::vector<double> propagationLatency;
        std::vector<double> visibilityLatency;
        for (unsigned int i = 0; i < _recordedLatencies.size(); ++i) {

            ReplicationLatencyResult &r = _recordedLatencies[i];
            overallLatency.push_back(r.OverallLatency.Seconds * 1000 +
                                     r.OverallLatency.NanoSeconds / 1000000.0);
            propagationLatency.push_back(r.PropagationLatency.Seconds * 1000 +
                                         r.PropagationLatency.NanoSeconds / 1000000.0);
            visibilityLatency.push_back(r.VisibilityLatency.Seconds * 1000 +
                                        r.VisibilityLatency.NanoSeconds / 1000000.0);
        }
        std::sort(overallLatency.begin(), overallLatency.end());
        std::sort(propagationLatency.begin(), propagationLatency.end());
        std::sort(visibilityLatency.begin(), visibilityLatency.end());

        //We take 10-20-30-40-50-60-70-80-90-95-99-th percentile
        double size = overallLatency.size() / 100.0;
        int i;

        for (i = 1; i <= 9; i++) {
            v.overall[i-1] = overallLatency[(int)(i * 10.0 * size)];
        }
        v.overall[9] = overallLatency[(int)(95.0 * size)];
        v.overall[10] = overallLatency[(int)(99.0 * size)];

        for (i = 1; i <= 9; i++) {
             v.propagation[i-1] = propagationLatency[(int)(i * 10.0 * size)];
        }
        v.propagation[9] = propagationLatency[(int)(95.0 * size)];
        v.propagation[10] = propagationLatency[(int)(99.0 * size)];

        for (i = 1; i <= 9; i++) {
            v.visibility[i-1] = visibilityLatency[(int)(i * 10.0 * size)];
        }
        v.visibility[9] = visibilityLatency[(int)(95.0 * size)];
        v.visibility[10] = visibilityLatency[(int)(99.0 * size)];

#endif
    }

    int MVKVStore::Size() {
        int size = 0;
        for (int i = 0; i < SysConfig::NumItemIndexTables; i++) {
            std::lock_guard <std::mutex> lk(*(_indexTableMutexes[i]));
            size += _indexTables[i]->size();
        }

        return size;
    }

    void MVKVStore::Reserve(int numItems) {
        for (int i = 0; i < SysConfig::NumItemIndexTables; i++) {
            std::lock_guard <std::mutex> lk(*(_indexTableMutexes[i]));
            _indexTables[i]->reserve(numItems / SysConfig::NumItemIndexTables + 1);
        }
    }

//////////////////////////////////////////////////////

    bool MVKVStore::HandlePropagatedUpdate(std::vector < PropagatedUpdate * > &updates) {
        for (unsigned int i = 0; i < updates.size(); i++) {
            InstallPropagatedUpdate(updates[i]);
        }

#ifdef MEASURE_STATISTICS
        SysStats::NumReceivedPropagatedUpdates += updates.size();
        SysStats::NumReceivedBatches += 1;
#endif

        return true;
    }

    bool MVKVStore::HandleHeartbeat(Heartbeat &hb, int srcReplica) {
        PVV_LOCK(srcReplica);

        assert(hb.PhysicalTime > _PVV[srcReplica]);
        assert(hb.LogicalTime >= _LVV[srcReplica]);
        _PVV[srcReplica] = hb.PhysicalTime;
#ifdef PVV_COND_VAR
        _pvv_waitHandle[srcReplica]->wakeUp();
#endif
        _LVV[srcReplica] = hb.LogicalTime;
        PVV_UNLOCK(srcReplica);
        return true;
    }

    void MVKVStore::InstallPropagatedUpdate(PropagatedUpdate *update) {
        ItemAnchor *itemAnchor = NULL;
        PhysicalTimeSpec depTime;

        int ti = Utils::strhash(update->Key) % SysConfig::NumItemIndexTables;
        if (ti < 0) ti += SysConfig::NumItemIndexTables;

        {
            std::lock_guard <std::mutex> lk(*(_indexTableMutexes[ti]));
            try {
                itemAnchor = _indexTables[ti]->at(update->Key);
            } catch (std::out_of_range &e) {
                if (SysConfig::Debugging) {
                    SLOG((boost::format("MVKVStore: ApplyPropagatedUpdate key %s does not exist.")
                          % update->Key).str());
                    assert(false);
                }
            }
        }


        // create and insert propagated version
        // no need to delete
        ItemVersion *newVersion = new ItemVersion(update->Value);
        //newVersion->LUT = update->LUT;
        newVersion->PUT = update->PUT;
        //newVersion->SrcPartition = update->SrcPartition;
        newVersion->SrcReplica = update->SrcReplica;
        newVersion->Persisted = false;
        PhysicalTimeSpec currentTime;

#ifndef DEP_VECTORS
        if (SysConfig::OptimisticMode) {
            newVersion->PDUT = update->PDUT;

#ifdef WAIT_REPLICATION
            //In pess-GR we do not need to wait before replicating. This is because a version is simply not visible until
            //Its update time gets past the GST so it is impossible to establish dependencies in the future
            PhysicalTimeDep depTime;
            FROM_64(depTime,newVersion->PDUT);
             // we don't need this in the PESS mode because in the PESS mode the update time is used in the visibility condition
                while ( depTime > (currentTime = Utils::GetCurrentClockTime())) {
                    PhysicalTimeSpec sleepTime = depTime - currentTime;
                    int sleep_ms = sleepTime.Seconds * 1000000 + sleepTime.NanoSeconds / 1000 + 1;
                std::this_thread::sleep_for(std::chrono::microseconds(sleep_ms));
                }

#endif //WAIT_REPLICATION
        }

#else //Dep vectors

        PhysicalTimeSpec maxDVTime(0, 0);
        newVersion->DV.resize(update->DV.size());
        for (int i = 0; i < update->DV.size(); i++) {
            //PhysicalTimeSpec p(update->DV[i].Seconds, update->DV[i].NanoSeconds);
            newVersion->DV[i].Seconds = update->DV[i].Seconds;
            newVersion->DV[i].NanoSeconds = update->DV[i].NanoSeconds;
#ifdef WAIT_REPLICATION
            assert(false); //Recheck how to apply this
            if (p > maxDVTime) {
                maxDVTime = p;
            }
#endif
        }

#ifdef WAIT_REPLICATION
    if (SysConfig::OptimisticMode) {

           FROM_64(depTime,maxDVTime);
        while (depTime > (currentTime = Utils::GetCurrentClockTime())) {

            PhysicalTimeSpec sleepTime = depTime - currentTime;

            int sleep_ms = sleepTime.Seconds * 1000000 + sleepTime.NanoSeconds / 1000 + 1;
            std::this_thread::sleep_for(std::chrono::microseconds(sleep_ms));

        }
    }
#endif //WAIT_REPLICATION


#endif //DEP_VECTORS


        update->UpdatedItemAnchor = itemAnchor;
        update->UpdatedItemVersion = newVersion;

        newVersion->RIT = Utils::GetCurrentClockTime();

        itemAnchor->InsertVersion(newVersion);

#ifdef MEASURE_STATISTICS
        SysStats::NumPendingPropagatedUpdates += 1;
#endif

        // persist operation
        //        LogManager::Instance()->AppendReplicatedUpdate(update); //do not uncomment this

        newVersion->Persisted = true;

        {
            PVV_LOCK(update->SrcReplica);
            _replicaClock += 1;

            if (_PVV[update->SrcReplica] >= update->PUT)
                assert(false);
            _PVV[update->SrcReplica] = update->PUT; //;MAX(update->PUT,_PVV[update->SrcReplica]);
#ifdef PVV_COND_VAR
            _pvv_waitHandle[update->SrcReplica]->wakeUp();
#endif
            PVV_UNLOCK(update->SrcReplica);
        }

        delete update;

#ifdef MEASURE_VISIBILITY_LATENCY
        {
            std::lock_guard<std::mutex> lk(_pendingInvisibleVersionsMutex);
           _pendingInvisibleVersions[newVersion->SrcReplica].push(newVersion);
        }
#endif
    }


    void MVKVStore::CheckAndRecordVisibilityLatency() {
        PhysicalTimeSpec currentTime = Utils::GetCurrentClockTime();
#ifndef DEP_VECTORS
        PhysicalTimeSpec gst = _GST;
#else  //DEP_VECT_YES
        std::vector<PhysicalTimeSpec> gsv = _GSV;
#endif     //DEP_VECTORS

        {
            std::lock_guard <std::mutex> lk(_pendingInvisibleVersionsMutex);
            PhysicalTimeSpec rit;
            PhysicalTimeSpec put;
            int j;
            bool vis;
            for (unsigned int i = 0; i < _pendingInvisibleVersions.size(); ++i) {
                std::queue < ItemVersion * > &q = _pendingInvisibleVersions[i];
                vis = true;
                while (!q.empty()) {
                    ItemVersion *version = q.front();

#ifndef DEP_VECTORS
                    if (version->PUT > gst) {
                        vis = false;
                    }
#else //VECTOR
                    for (j = 0; j < version->DV.size(); j++) {
                        if (version->DV[j] > gsv[j]) {
                            vis = false;
                            break;
                        }
                    }
#endif //DEP_VECTORS

                    if (!vis) {
                        break;
                    }

                    _numCheckedVersions += 1;

                    if (_numCheckedVersions % SysConfig::LatencySampleInterval == 0) {
                        put = version->PUT;

                        PhysicalTimeSpec put64 = put;
                        FROM_64(put, put);

                        ReplicationLatencyResult result;
                        result.OverallLatency = currentTime - put;
                        result.PropagationLatency = version->RIT - put;
                        result.VisibilityLatency = currentTime - version->RIT;
                        result.SrcReplica = version->SrcReplica;
                        _recordedLatencies.push_back(result);
                    }

                    q.pop();
                }
            }
        }
    }


    void MVKVStore::calculateStalenessStatistics(StalenessStatistics &stat) {

        SLOG("Entering calculateStalenessStatistics");
        std::vector <int64_t> numFreshVer;
        {
            std::lock_guard <std::mutex> lk(SysStats::NumFresherVersionsInItemChainMutex);
            numFreshVer = SysStats::NumFresherVersionsInItemChain;
        }

        if (numFreshVer.size() == 0) {
            stat.averageNumFresherVersionsInItemChain = 0;
        } else {
            stat.averageNumFresherVersionsInItemChain = std::accumulate(
                    numFreshVer.begin(), numFreshVer.end(), 0.0) / numFreshVer.size();
            assert(stat.averageNumFresherVersionsInItemChain >= 1);
        }

        std::vector<double> usrPercST;

        {
            std::lock_guard <std::mutex> lk(SysStats::UserPerceivedStalenessTimeMutex);
            usrPercST = SysStats::UserPerceivedStalenessTime;
        }

        if (usrPercST.size() == 0) {
            stat.averageUserPerceivedStalenessTime = 0;
            stat.minStalenessTime = 0;
            stat.maxStalenessTime = 0;
            stat.medianStalenessTime = 0;
            stat._90PercentileStalenessTime = 0;
            stat._95PercentileStalenessTime = 0;
            stat._99PercentileStalenessTime = 0;

        } else {

            stat.averageUserPerceivedStalenessTime =
                    std::accumulate(usrPercST.begin(), usrPercST.end(), 0.0) / usrPercST.size();

            std::sort(usrPercST.begin(), usrPercST.end());
            stat.minStalenessTime = usrPercST.at(0);
            stat.maxStalenessTime = usrPercST.at(usrPercST.size() - 1);
            stat.medianStalenessTime = usrPercST.at((int) (usrPercST.size() * 0.50));
            stat._90PercentileStalenessTime = usrPercST.at((int) (usrPercST.size() * 0.90));
            stat._95PercentileStalenessTime = usrPercST.at((int) (usrPercST.size() * 0.95));
            stat._99PercentileStalenessTime = usrPercST.at((int) (usrPercST.size() * 0.99));
        }

        SLOG("Exiting calculateStalenessStatistics");
    }

    void MVKVStore::calculateUnmergedVersionsStatistics(UnmergedVersionsStatistics &stat) {

        SLOG("Entering calculateUnmergedVersionsStatistics");


        std::vector <int64_t> numUnmergedVer;

        {
            std::lock_guard <std::mutex> lk(SysStats::NumUnmergedVersionsInItemChainMutex);
            numUnmergedVer = SysStats::NumUnmergedVersionsInItemChain;
        }

        if (numUnmergedVer.size() == 0) {
            stat.average = 0;
            stat.value = 0;

        } else {
            stat.value = std::accumulate(
                    numUnmergedVer.begin(), numUnmergedVer.end(), 0.0);
            stat.average = stat.value / numUnmergedVer.size();

        }

        SLOG("Exiting calculateUnmergedVersionsStatistics");
    }

    void MVKVStore::addStalenessTimesToOutput(string &str) {
        {
            str += "\n\nSTALENESS TIMES\n\n";
            std::lock_guard <std::mutex> lk(SysStats::UserPerceivedStalenessTimeMutex);

            for (int i = 0; i < SysStats::UserPerceivedStalenessTime.size(); i++) {
                str += ((boost::format("%lf\n") % SysStats::UserPerceivedStalenessTime[i]).str());
            }

        }

    }

    void MVKVStore::calculateBlockStatistics(OptVersionBlockStatistics &stat) {

        SLOG("Entering calculateBlockStatistics");

        std::vector<double> blockTime;

        {
            std::lock_guard <std::mutex> lk(SysStats::BlockDurationMutex);

            ASSERT(SysStats::BlockDuration.size() == SysStats::NumBlocks);
            blockTime = SysStats::BlockDuration;

        }

        if (blockTime.size() == 0) {
            stat.setToZero();
        } else {
            stat.average =
                    std::accumulate(blockTime.begin(), blockTime.end(), 0.0) / blockTime.size();
        }


        if (blockTime.size() != 0) {
            std::sort(blockTime.begin(), blockTime.end());
            stat.min = *std::min_element(blockTime.begin(), blockTime.end());
            stat.max = *std::max_element(blockTime.begin(), blockTime.end());
            if (blockTime.size() == 1) {
                stat.median = blockTime.at(0);
                stat._90Percentile = blockTime.at(0);
                stat._95Percentile = blockTime.at(0);
                stat._99Percentile = blockTime.at(0);
            } else {
                stat.median = blockTime.at((int) std::round(blockTime.size() * 0.50));
                stat._90Percentile = blockTime.at((int) std::round(blockTime.size() * 0.90) - 1);
                stat._95Percentile = blockTime.at((int) std::round(blockTime.size() * 0.95) - 1);
                stat._99Percentile = blockTime.at((int) std::round(blockTime.size() * 0.99) - 1);
            }
        }

        SLOG("Exiting calculateBlockStatistics");


    }

    void MVKVStore::UpdateVVIfSmaller(PhysicalTimeSpec ts) {

        PVV_LOCK(_replicaId);
        if (ts > _PVV[_replicaId]) { //&& ts > Utils::GetCurrentClockTime()) {
            TO_64(_PVV[_replicaId], ts);
        }
        PVV_UNLOCK(_replicaId);
    }

    void MVKVStore::CreateBlockDurationPercentileStats(BlockDurationPercentilesStatistics &v) {

        SLOG("Entering CreateBlockDurationPercentileStats.");
        if (SysStats::BlockDuration.size() > 0) {
            std::sort(SysStats::BlockDuration.begin(), SysStats::BlockDuration.end());

            //We take 10-20-30-40-50-60-70-80-90-95-99-th percentile
            double size = SysStats::BlockDuration.size() / 100.0;
            int i;

            for (i = 1; i <= 9; i++) {
                v.percentiles[i - 1] = SysStats::BlockDuration[(int) (i * 10.0 * size)];
            }
            v.percentiles[9] = SysStats::BlockDuration[(int) (95.0 * size)];
            v.percentiles[10] = SysStats::BlockDuration[(int) (99.0 * size)];
        } else {
            for (int i = 1; i <= 9; i++) {
                v.percentiles[i - 1] = -1;
            }
            v.percentiles[9] = -1;
            v.percentiles[10] = -1;
        }

        SLOG("Exiting CreateBlockDurationPercentileStats.");

    }


} // namespace scc
