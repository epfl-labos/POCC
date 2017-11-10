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


#include "kvservice/coordinator.h"
#include "kvstore/mv_kvstore.h"
#include "kvstore/log_manager.h"
#include "common/sys_logger.h"
#include "common/utils.h"
#include "common/types.h"
#include "common/sys_stats.h"
#include "common/sys_config.h"
#include <thread>
#include <unordered_map>
#include <queue>
#include <pthread.h>
#include <sched.h>
#include <iostream>

//#define DEP_VECTORS
//#define PARALLEL_XACTS
#ifdef AVOID_GST
#ifndef DEP_VECTORS
#error Avoid GST ONLY DEFINED WITH VECTORS
#endif
#endif

#ifndef LOCAL_LINEARIZABLE
#error LOCAL_LINEARIZABLE MUST BE TURNED ON
#endif


namespace scc {

    Coordinator::Coordinator(std::string name, int publicPort, int totalNumKeys)
            : _currentPartition(name, publicPort),
              _partitionId(0),
              _replicaId(0),
              _totalNumPreloadedKeys(totalNumKeys),
              _isDistributed(false),
              _groupClient(NULL),
              _readyToServeRequests(false),
              _delta(0, SysConfig::OpttxDelta)
#ifdef PARALLEL_XACTS
    , _xact_id(0),
      _xact_lock(0)
#endif
    {
        TO_64(_delta, _delta);
    }

    Coordinator::Coordinator(std::string serverName,
                             int publicPort,
                             int partitionPort,
                             int replicationPort,
                             int partitionId,
                             int replicaId,
                             int totalNumKeys,
                             std::string groupServerName,
                             int groupServerPort)
            : _currentPartition(serverName, publicPort, partitionPort,
                                replicationPort, partitionId, replicaId),
              _partitionId(partitionId),
              _replicaId(replicaId),
              _totalNumPreloadedKeys(totalNumKeys),
              _isDistributed(true),
              _groupClient(NULL),
              _readyToServeRequests(false),
              _delta(0, SysConfig::OpttxDelta)
#ifdef PARALLEL_XACTS
    , _xact_id(0), _xact_lock(0)
#endif
    {
        fprintf(stdout, "Coordinator. Partition %d Replica %d\n", _partitionId, _replicaId);
        _groupClient = new GroupClient(groupServerName, groupServerPort);
        TO_64(_delta, _delta);
    }


    Coordinator::~Coordinator() {
        if (_isDistributed) {
            delete _groupClient;
            for (unsigned int i = 0; i < _myReplicas.size(); i++) {
                delete _replicationClients[_myReplicas[i].ReplicaId];
            }
            for (unsigned int i = 0; i < _myPartitions.size(); i++) {
                delete _partitionClients[_myPartitions[i].PartitionId];
            }
        }
    }

    void Coordinator::Initialize() {
        ItemAnchor::_replicaId = _currentPartition.ReplicaId;

        if (!_isDistributed) {
            for (int i = 0; i < _totalNumPreloadedKeys; i++) {
                std::string key = std::to_string(i);
                std::string value(8, 'x'); // default value size is hard coded 
                MVKVStore::Instance()->Add(key, value);
            }

            int keyCount = MVKVStore::Instance()->Size();
            SLOG((boost::format("Loaded all %d keys.") % keyCount).str());

            _readyToServeRequests = true;
        } else {
            // register at the group manager
            _groupClient->RegisterPartition(_currentPartition);

            // get all partitions in the system
            _allPartitions = _groupClient->GetRegisteredPartitions();
            _numPartitions = _allPartitions.size();
            _numReplicasPerPartition = _allPartitions[0].size();
            for (int i = 0; i < _numReplicasPerPartition; i++) {
                if (i != _currentPartition.ReplicaId) {
                    _myReplicas.push_back(_allPartitions[_currentPartition.PartitionId][i]);
                }
            }
            for (int i = 0; i < _numPartitions; i++) {
                if (i != _currentPartition.PartitionId) {
                    _myPartitions.push_back(_allPartitions[i][_currentPartition.ReplicaId]);
                }
            }

            // initialize Log Manager
            LogManager::Instance()->Initialize(_numReplicasPerPartition);

            // initialize MVKVStore
            MVKVStore::Instance()->SetPartitionInfo(_currentPartition,
                                                    _numPartitions,
                                                    _numReplicasPerPartition,
                                                    _totalNumPreloadedKeys);

            MVKVStore::Instance()->Initialize();
            SLOG("MVKVStore initialized\n");

            // load keys
            for (int i = 0; i < _totalNumPreloadedKeys; i++) {

                if (i % _numPartitions == _currentPartition.PartitionId) {
                    std::string key = std::to_string(i);
                    std::string value(8, 'x'); // default value size is hard coded

                    MVKVStore::Instance()->Add(key, value);
                }
            }

            int keyCount = MVKVStore::Instance()->Size();
            SLOG((boost::format("Loaded all %d keys.") % keyCount).str());

            // connect to remote replicas
            for (unsigned int i = 0; i < _myReplicas.size(); i++) {
                DBPartition &p = _myReplicas[i];
                ReplicationKVClient *client = new ReplicationKVClient(p.Name, p.ReplicationPort, _replicaId);
                _replicationClients[p.ReplicaId] = client;
            }
            SLOG("Connected to all remote replicas.");

            // connect to local partitions
            for (unsigned int i = 0; i < _myPartitions.size(); i++) {
                DBPartition &p = _myPartitions[i];
                PartitionKVClient *client = new PartitionKVClient(p.Name, p.PartitionPort);
                _partitionClients[p.PartitionId] = client;
            }
            SLOG("Connected to all local partitions.");
#ifdef AVOID_GST
            SLOG(" === Disabling GST/GSV computation ===");
            if(false){
#else

            if (!SysConfig::OptimisticMode) {
#endif

                if (SysConfig::GSTDerivationMode == GSTDerivationType::TREE) {
                    // build GST tree
                    BuildGSTTree();
                    SLOG("Built GST tree.");
                    if (_currentTreeNode->NodeType == TreeNodeType::RootNode) {
                        {
                            // launch GST sending thread if current node is root
                            std::thread t(&Coordinator::SendGSTAtRoot, this);

                            sched_param sch;
                            int policy;
                            pthread_getschedparam(t.native_handle(), &policy, &sch);
                            sch.sched_priority = sched_get_priority_max(SCHED_FIFO);
                            if (pthread_setschedparam(t.native_handle(), SCHED_FIFO, &sch)) {
                                std::cout << "Failed to set schedparam: " << strerror(errno) << '\n';
                            }

                            t.detach();
                        }
                    }

                } else {
                    std::cout << "Non-existing GSTDerivationType: \n";
                }
            }

            SLOG("Launch update propagation thread");
            // launch update propagation thread

            {
                std::thread t(&Coordinator::PropagatePersistedUpdate, this);
                t.detach();
            }

            // I'm ready! Notify the group manager
            _groupClient->NotifyReadiness(_currentPartition);

            std::this_thread::sleep_for(std::chrono::milliseconds(500));

            _readyToServeRequests = true;

            SLOG("Ready to serve client requests.");
        }
    }

    void Coordinator::Echo(const std::string &input,
                           std::string &output) {
        output = input;
    }

    bool Coordinator::Add(const std::string &key, const std::string &value) {
        return MVKVStore::Instance()->Add(key, value);
    }


    bool Coordinator::Get(ConsistencyMetadata &cdata,
                          const std::string &key,
                          std::string &value) {
#ifdef MEASURE_STATISTICS
        SysStats::NumPublicGetRequests += 1;
#endif

        int partitionId = std::stoi(key) % _numPartitions;
        if (partitionId == _currentPartition.PartitionId) {
            return MVKVStore::Instance()->Get(cdata, key, value);
        } else {
            return _partitionClients[partitionId]->Get(cdata, key, value);
        }
    }

    bool Coordinator::Set(ConsistencyMetadata &cdata,
                          const std::string &key,
                          const std::string &value) {

#ifdef MEASURE_STATISTICS
        SysStats::NumPublicSetRequests += 1;
#endif

        int partitionId = std::stoi(key) % _numPartitions;
        if (partitionId == _currentPartition.PartitionId) {
            return MVKVStore::Instance()->Set(cdata, key, value);
        } else {
            return _partitionClients[partitionId]->Set(cdata, key, value);
        }
    }


#ifndef DEP_VECTORS //SCALAR


#ifdef PARALLEL_XACTS
    bool Coordinator::ParallelTxGet(ConsistencyMetadata &cdata,
                                    const std::vector <std::string> &keySet,
                                    std::vector <std::string> &valueSet) {

#ifndef LOCAL_LINEARIZABLE
#error Local linearizable is mandatory
#endif

        MVKVStore::Instance()->SetGSTIfSmaller(cdata.GST);
        bool ret = true;
#ifdef MEASURE_STATISTICS
        bool slept = false;
        SysStats::NumXacts++;
        PhysicalTimeSpec initWait;
#endif

        while (cdata.DT > (cdata.GST = MVKVStore::Instance()->GetGST())) {
#ifdef MEASURE_STATISTICS
            if(!slept){
                slept =true;
                initWait = (Utils::GetCurrentClockTime());
           }
#endif
            std::this_thread::sleep_for(std::chrono::microseconds(SysConfig::GetSpinTime));
        }
#ifdef MEASURE_STATISTICS
        if(slept){
                SysStats::NumXactBlocks++;
                PhysicalTimeSpec endWait = Utils::GetCurrentClockTime();
                endWait = endWait - initWait;
                SysStats::XactBlockDuration+=(long)(1000.0 * endWait.toMilliSeconds());
        }
#endif
        //For now, there is one key per touched partition, and not more
        const int cohort_size = keySet.size();
        //1. Setup data structures
        ParallelXact p_x(0,cohort_size);
        // 2. Register the object

        LOCK_XACT();
        p_x.xact_id = _xact_id++;
        std::pair<unsigned long,ParallelXact*> ptr (p_x.xact_id,&p_x);
        _xacts_ptrs.insert(ptr);
        UNLOCK_XACT();

        for(unsigned int i=0;i<cohort_size;i++){
            ConsistencyMetadata *slice_cdata = new ConsistencyMetadata();
            slice_cdata->GST = cdata.GST;
            INITIALIZE_METADATA(p_x,slice_cdata,i);
        }

        int local_key_id = -1;
        int partitionId ;
        std::string key;
        for (unsigned int i = 0; i < keySet.size(); ++i) {
            key = keySet[i];
            partitionId = std::stoi(key) % _numPartitions;
            SET_PART_TO_KEY(p_x,partitionId,i);
            SET_KEY_TO_PART(p_x,i,partitionId);
            if (partitionId == _currentPartition.PartitionId) {
                local_key_id = i;
            }
        }
        double max_slept = 0;

        for (unsigned int i = 0; i < keySet.size(); ++i) {
            key = keySet[i];
            partitionId = std::stoi(key) % _numPartitions;
            if(i!=local_key_id){
                _partitionClients[partitionId]->ParallelTxSliceGet(*p_x._slices[i]->metadata, key, p_x._slices[i]->value, p_x.xact_id,_partitionId);
            }

        }
        //Process local if needed
        if(local_key_id>-1){
            MVKVStore::Instance()->LocalTxSliceGet(*(p_x._slices[local_key_id]->metadata), keySet[local_key_id], p_x, local_key_id);
            p_x._waitHandle.DecrementAndWaitIfNonZero();
        }
        else{
            p_x._waitHandle.WaitIfNonZero();
        }

        //Process replies
        for (unsigned int i = 0; i < keySet.size(); ++i) {
            if(GET_RET(p_x,i)){
                cdata.DT = MAX(cdata.DT,GET_DT(p_x,i));
                valueSet.push_back(GET_VALUE(p_x,i));
            }
        }

        LOCK_XACT();
        _xacts_ptrs.erase(p_x.xact_id);
        UNLOCK_XACT();

        return ret;
    }
#endif  //PARALLEL_XACT


    bool Coordinator::TxGet(ConsistencyMetadata &cdata, const std::vector <std::string> &keySet,
                            std::vector <std::string> &valueSet) {
#ifdef LOCAL_LINEARIZABLE
        MVKVStore::Instance()->SetGSTIfSmaller(cdata.GST);
#endif

        bool ret = true;
        PhysicalTimeSpec snapshotTime;
#ifdef MEASURE_STATISTICS
        bool slept = false;
        SysStats::NumXacts++;
#endif
        PhysicalTimeSpec initWait;
        //1. Wait until dt > GST (GST is the max between the GST at the server and the highest GST seen by the client so far)
#ifndef LOCAL_LINEARIZABLE
        while (cdata.DT > (snapshotTime = MAX(cdata.GST, MVKVStore::Instance()->GetGST()))) {
#else
            while (cdata.DT > (snapshotTime = MVKVStore::Instance()->GetGST())) {
#endif

#ifdef MEASURE_STATISTICS
            if(!slept){
                slept =true;
                initWait = (Utils::GetCurrentClockTime());
           }
#endif

            std::this_thread::sleep_for(std::chrono::microseconds(SysConfig::GetSpinTime));
        }

#ifdef MEASURE_STATISTICS
        if(slept){
                SysStats::NumXactBlocks++;
                PhysicalTimeSpec endWait = Utils::GetCurrentClockTime();
                endWait = endWait - initWait;
                SysStats::XactBlockDuration+=(long)(1000.0 * endWait.toMilliSeconds());
        }
#endif
        ConsistencyMetadata slice_cdata;
        for (unsigned int i = 0; i < keySet.size(); ++i) {
            //SLice_cdata is used both for input and output of SliceGet
            slice_cdata.GST = snapshotTime; //This is passed to TxSliceGet as input GST

            std::string value;
            //First argument is used to supply params but also to get them back
            ret = TxSliceGet(slice_cdata, keySet[i], value);
            //So now slice_data has the returned GST and the returned DT
            if (!ret) {
                SLOG("TxSliceGetPess returned false");
                break;
            }
            //Freshest UT among the read items
            cdata.DT = MAX(cdata.DT, slice_cdata.DT);
            valueSet.push_back(value);
        }
        cdata.GST = snapshotTime;
        return ret;
    }

#else //VECTOR

    PhysicalTimeSpec Coordinator::UpdateGSVAndComputeTransactionVector(ConsistencyMetadata &cdata) {
#ifdef LOCAL_LINEARIZABLE
        MVKVStore::Instance()->SetGSVIfSmaller(cdata.GSV);
        //We set the GSV now
        cdata.GSV= MVKVStore::Instance()->GetGSV(); //This is going to be returned to the client
#endif //LOCAL_LINEARIZABLE

        PhysicalTimeSpec localSnapshotTime = cdata.DT; //This the the highest LOCAL dependency (remote deps are at most as GSV[i])

        PhysicalTimeSpec currentTime = Utils::GetCurrentClockTime();
        PhysicalTimeSpec temp = currentTime;
        TO_64(currentTime, currentTime);
        //Compute the snapshot vector
        //The remote entries are the current GSV (already maxed with client's one)
        //The local entry is either cdata.DT or Clock_n^m. It cannot be GSV[local], as it is the lower bound in the DC
        localSnapshotTime = MAX(localSnapshotTime, currentTime);
        return localSnapshotTime;
    }

    //This is the version that computes the snapshot vector on the coordinator
    //And waits on all the slaves
    bool Coordinator::TxGet(ConsistencyMetadata &cdata,
                            const std::vector<std::string> &keySet,
                            std::vector<std::string> &valueSet) {
        bool ret = true;
        PhysicalTimeSpec localSnapshotTime;
        PhysicalTimeVector transactionVector;

        if (SysConfig::OptimisticMode) {
            localSnapshotTime = cdata.DT;
            transactionVector = MVKVStore::Instance()->GetPVV();
            transactionVector = MAX(transactionVector, cdata.NDV);
            cdata.DV.resize(transactionVector.size());
            cdata.GSV = transactionVector;
        } else {
            localSnapshotTime = UpdateGSVAndComputeTransactionVector(cdata);
            cdata.DV.resize(cdata.GSV.size());
        }

        ConsistencyMetadata slice_cdata;

        if (SysConfig::OptimisticMode) {
            slice_cdata.GSV = transactionVector;
        } else {
            //Only done once since GSV is not overridden by return values
            slice_cdata.GSV = cdata.GSV;
        }
        double max_slept = 0;
        for (unsigned int i = 0; i < keySet.size(); ++i) {
            //SLice_cdata is used both for input and otput of SliceGet
            slice_cdata.DT = localSnapshotTime;
            std::string value;

            //First argument is used to supply params but also to get them back
            ret = TxSliceGet(slice_cdata, keySet[i], value);

            //So now slice_data has the returned GST and the returned DT
            if (!ret) {
                SLOG("TxSliceGet returned false");
                break;
            }
            //Freshest UT among the read items
            cdata.DV[slice_cdata.SrcReplica] = MAX(cdata.DV[slice_cdata.SrcReplica], slice_cdata.DT);
            //We do not recompute the GSV. The client's not gonna see anything that depends on anything newer than snapshotTime
            valueSet.push_back(value);
#ifdef MEASURE_STATISTICS
            max_slept = MAX(max_slept, slice_cdata.waited_xact);
#endif
        }
#ifdef MEASURE_STATISTICS
        SysStats::NumXacts++;
        if (max_slept > 0) {
            SysStats::XactBlockDuration += (long) (max_slept); //usec
            SysStats::NumXactBlocks++;
        }
#endif
        return ret;
    }


#ifdef PARALLEL_XACTS

    bool Coordinator::ParallelTxGet(ConsistencyMetadata &cdata,
                                    const std::vector<std::string> &keySet,
                                    std::vector<std::string> &valueSet) {
        bool ret = true;
        PhysicalTimeSpec localSnapshotTime;
        PhysicalTimeVector transactionVector;

        if (SysConfig::OptimisticMode) {
            localSnapshotTime = cdata.DT;
            transactionVector = MVKVStore::Instance()->GetPVV();
            transactionVector = MAX(transactionVector, cdata.NDV);
            cdata.DV.resize(transactionVector.size());
            cdata.GSV = transactionVector;
        } else {
            localSnapshotTime = UpdateGSVAndComputeTransactionVector(cdata);
            cdata.DV.resize(cdata.GSV.size());
        }

        //For now, there is one key per touched partition, and not more
        const int cohort_size = keySet.size();
        //1. Setup data structures
        ParallelXact p_x(0, cohort_size);
        // 2. Register the object

        LOCK_XACT();
            p_x.xact_id = _xact_id++;
            std::pair<unsigned long, ParallelXact *> ptr(p_x.xact_id, &p_x);
            _xacts_ptrs.insert(ptr);
        UNLOCK_XACT();

        for (unsigned int i = 0; i < cohort_size; i++) {
            ConsistencyMetadata *slice_cdata = new ConsistencyMetadata();
            slice_cdata->DT = localSnapshotTime;

            if (SysConfig::OptimisticMode) {
                slice_cdata->GSV = transactionVector;
            } else {
                //Only done once since GSV is not overridden by return values
                slice_cdata->GSV = cdata.GSV;
            }
            INITIALIZE_METADATA(p_x, slice_cdata, i);
        }

        int local_key_id = -1;
        int partitionId;
        std::string key;
        for (unsigned int i = 0; i < keySet.size(); ++i) {
            key = keySet[i];
            partitionId = std::stoi(key) % _numPartitions;
            SET_PART_TO_KEY(p_x, partitionId, i);
            SET_KEY_TO_PART(p_x, i, partitionId);
            if (partitionId == _currentPartition.PartitionId) {
                local_key_id = i;
            }
        }
        double max_slept = 0;
        //Now all vectors are populated, so they can be written concurrently

        for (unsigned int i = 0; i < keySet.size(); ++i) {
            key = keySet[i];
            partitionId = std::stoi(key) % _numPartitions;
            if (i != local_key_id) {
                _partitionClients[partitionId]->ParallelTxSliceGet(*p_x._slices[i]->metadata, key,
                                                                   p_x._slices[i]->value, p_x.xact_id, _partitionId);
            }

        }
        //Process local if needed
        if (local_key_id > -1) {
            PhysWaitOnTxSlice(*(p_x._slices[local_key_id]->metadata));

            MVKVStore::Instance()->LocalTxSliceGet(*(p_x._slices[local_key_id]->metadata), keySet[local_key_id], p_x,local_key_id);
            p_x._waitHandle.DecrementAndWaitIfNonZero();
        } else {
            p_x._waitHandle.WaitIfNonZero();
        }

        //Process replies
        int srcReplica;
        for (unsigned int i = 0; i < keySet.size(); ++i) {
            if (GET_RET(p_x, i)) {
                srcReplica = GET_SRC_REPLICA(p_x, i);
                cdata.DV[srcReplica] = MAX(cdata.DV[srcReplica], GET_DT(p_x, i));
                valueSet.push_back(GET_VALUE(p_x, i));
#ifdef MEASURE_STATISTICS
                max_slept = MAX(max_slept, GET_SLEEP(p_x,i));
#endif
            }
        }
#ifdef MEASURE_STATISTICS
        SysStats::NumXacts++;
        if(max_slept > 0){
            SysStats::XactBlockDuration+= (long)(max_slept*1000.0);
            SysStats::NumXactBlocks++;
    }
#endif
        LOCK_XACT();
            _xacts_ptrs.erase(p_x.xact_id);
        UNLOCK_XACT();

        return ret;
    }

#endif //PARALLEL_XACTS

#endif


#ifdef DEP_VECTORS

    void Coordinator::WaitOnTxSlice(ConsistencyMetadata &cdata) {
        PhysWaitOnTxSlice(cdata);
#ifdef MEASURE_STATISTICS
      cdata.waited_xact = 0;
#endif//STATS
    }

    void Coordinator::PhysWaitOnTxSlice(ConsistencyMetadata &cdata) {
#ifdef FAKE_TX_WAIT
        cdata.waited_xact = 0;
        return;
#endif
#ifdef MEASURE_STATISTICS
        bool slept = false;
        PhysicalTimeSpec initWait;
#endif
        PhysicalTimeSpec localSnapshotTime = cdata.DT;
        PhysicalTimeSpec currentTime, sleepTime;
        FROM_64(localSnapshotTime, localSnapshotTime);
        while (localSnapshotTime > (currentTime = Utils::GetCurrentClockTime())) {
#ifdef MEASURE_STATISTICS
            if (!slept) {
                slept = true;
                initWait = currentTime;
            }
#endif

            sleepTime = localSnapshotTime - currentTime;
            int sleep_ms = sleepTime.Seconds * 1000000 + sleepTime.NanoSeconds / 1000 + 1;
            std::this_thread::sleep_for(std::chrono::microseconds(sleep_ms));
        }

#ifdef MEASURE_STATISTICS
        if (slept) {
            PhysicalTimeSpec endWait = currentTime - initWait;
            cdata.waited_xact = (endWait.toMilliSeconds() * 1000.0);
        } else {
            cdata.waited_xact = 0;
        }
#endif //STATS
    }

#endif //DEP_VECTOR

    bool Coordinator::TxSliceGet(ConsistencyMetadata &cdata,
                                 const std::string &key,
                                 std::string &value) {

        int partitionId = std::stoi(key) % _numPartitions;
        if (partitionId == _currentPartition.PartitionId) {
#ifdef DEP_VECTORS  //In the scalar mode, the coordinator immediately waits
            PhysWaitOnTxSlice(cdata);
#endif //DEP_VECTORS
            return MVKVStore::Instance()->TxSliceGet(cdata, key, value);
        } else {
            return _partitionClients[partitionId]->TxSliceGet(cdata, key, value);
        }
    }

    bool Coordinator::ShowItem(const std::string &key, std::string &itemVersions) {
        int partitionId = std::stoi(key) % _numPartitions;
        if (partitionId == _currentPartition.PartitionId) {
            return MVKVStore::Instance()->ShowItem(key, itemVersions);
        } else {
            return _partitionClients[partitionId]->ShowItem(key, itemVersions);
        }
    }

    bool Coordinator::ShowDB(std::string &allItemVersions) {
        return MVKVStore::Instance()->ShowDB(allItemVersions);
    }

    bool Coordinator::ShowState(std::string &stateStr) {
        return MVKVStore::Instance()->ShowState(stateStr);
    }

    bool Coordinator::DumpLatencyMeasurement(std::string &resultStr) {
        return MVKVStore::Instance()->DumpLatencyMeasurement(resultStr);
    }

    bool Coordinator::InternalGet(ConsistencyMetadata &cdata,
                                  const std::string &key,
                                  std::string &value) {

#ifdef MEASURE_STATISTICS
        SysStats::NumInternalGetRequests += 1;
#endif
        return MVKVStore::Instance()->Get(cdata, key, value);
    }

    bool Coordinator::InternalSet(ConsistencyMetadata &cdata,
                                  const std::string &key,
                                  const std::string &value) {
#ifdef MEASURE_STATISTICS
        SysStats::NumInternalSetRequests += 1;
#endif
        return MVKVStore::Instance()->Set(cdata, key, value);
    }

    bool Coordinator::InternalTxSliceGet(ConsistencyMetadata &cdata,
                                         const std::string &key,
                                         std::string &value) {

#ifdef DEP_VECTORS  //In scalar GentleRain, only the coordinator waits
        WaitOnTxSlice(cdata);
#endif

        return MVKVStore::Instance()->TxSliceGet(cdata, key, value);
    }

////////////////// update replication //////////////////////////////

    bool Coordinator::HandlePropagatedUpdate(std::vector < PropagatedUpdate * > &updates) {
#ifdef MEASURE_STATISTICS
        if (!updates.empty()) {
            SysStats::NumRecvUpdateReplicationMsgs += 1;
            SysStats::NumRecvUpdateReplications += updates.size();
        }
#endif

        return MVKVStore::Instance()->HandlePropagatedUpdate(updates);
    }

    bool Coordinator::HandleHeartbeat(Heartbeat &hb, int srcReplica) {

#ifdef MEASURE_STATISTICS
        SysStats::NumRecvHeartbeats += 1;
#endif

        return MVKVStore::Instance()->HandleHeartbeat(hb, srcReplica);
    }


    void Coordinator::PropagatePersistedUpdate() {
        // initialize replication
        for (unsigned int i = 0; i < _myReplicas.size(); i++) {
            int rId = _myReplicas[i].ReplicaId;
            _replicationClients[rId]->InitializeReplication(_currentPartition);
        }

        int64_t lastLUT = 0;

        SLOG("Update propagation thread is up.");

        auto &workAvailable = LogManager::Instance()->ReplicationWaitHandle;

        auto &updateQueueMutex =
                LogManager::Instance()->ToPropagateLocalUpdateQueueMutex;
        auto &updateQueue =
                LogManager::Instance()->ToPropagateLocalUpdateQueue;

        std::vector < LocalUpdate * > *localUpdates;

        int maxWaitTime = SysConfig::ReplicationHeartbeatInterval; // microseconds

        // wait until all replicas are ready
        while (!_readyToServeRequests) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        while (true) {
            Shared lock(mtx);
            // true -> timeout
            bool ret = workAvailable.WaitAndResetWithTimeout(maxWaitTime);

            if (ret) {
                PhysicalTimeSpec physicalTime;
                int64_t logicalTime;

                MVKVStore::Instance()->GetSysClockTime(physicalTime, logicalTime);

                Heartbeat hb;
                hb.PhysicalTime = physicalTime;
                hb.LogicalTime = logicalTime;
                // send heartbeat
                if (logicalTime == lastLUT) {
                    for (unsigned int j = 0; j < _myReplicas.size(); j++) {
                        int rId = _myReplicas[j].ReplicaId;
                        _replicationClients[rId]->SendHeartbeat(hb);
                    }

                    continue;
                }
            } else {
                if (SysConfig::UpdatePropagationBatchTime > 0) {
                    std::this_thread::sleep_for(std::chrono::microseconds(
                            SysConfig::UpdatePropagationBatchTime));
                }

                {
                    std::lock_guard <std::mutex> lk(updateQueueMutex);


                    localUpdates = LogManager::Instance()->GetCurrUpdates();

#ifdef MEASURE_STATISTICS
                    SysStats::NumUPropagatedPersistedUpdates += localUpdates->size();
#endif
                    // send updates
                    for (unsigned int j = 0; j < _myReplicas.size(); j++) {
                        int rId = _myReplicas[j].ReplicaId;
                        _replicationClients[rId]->SendUpdate(*localUpdates);

#ifdef MEASURE_STATISTICS
                        SysStats::NumSentUpdates += localUpdates->size();
                        SysStats::NumSentBatches += 1;
#endif
                    }

                    if (!localUpdates->empty()) {
                        auto v = static_cast<ItemVersion *> (localUpdates->back()->UpdatedItemVersion);
                        lastLUT = v->LUT;
                    }

                    for (unsigned int i = 0; i < localUpdates->size(); i++) {
                        delete (*localUpdates)[i];
                    }
                    localUpdates->clear();
                }

            }
        }
    }

///////////////// GST Tree //////////////////////////////////////////

    void Coordinator::BuildGSTTree() {
        std::queue < TreeNode * > treeNodes;

        // root node
        int i = 0;
        _treeRoot = new TreeNode();
        _treeRoot->Name = _allPartitions[i][_replicaId].Name;
        _treeRoot->PartitionId = i;
        _treeRoot->Parent = nullptr;
        _treeRoot->PartitionClient = (i != _partitionId ? _partitionClients[i] : nullptr);

        treeNodes.push(_treeRoot);

        if (i == _partitionId) {
            _currentTreeNode = _treeRoot;
        }
        i += 1;

        // internal & leaf nodes
        while (!treeNodes.empty()) {
            TreeNode *parent = treeNodes.front();
            treeNodes.pop();

            for (int j = 0; j < SysConfig::NumChildrenOfTreeNode && i < _numPartitions; ++j) {
                TreeNode *node = new TreeNode();
                node->Name = _allPartitions[i][_replicaId].Name;
                node->PartitionId = i;
                node->Parent = parent;
                node->PartitionClient = (i != _partitionId ? _partitionClients[i] : nullptr);

                treeNodes.push(node);

                if (i == _partitionId) {
                    _currentTreeNode = node;
                }
                ++i;

                parent->Children.push_back(node);
            }
        }

        // set current node type
        std::string nodeTypeStr;
        if (_currentTreeNode->Parent == nullptr) {
            _currentTreeNode->NodeType = TreeNodeType::RootNode;
            nodeTypeStr = "root";
        } else if (_currentTreeNode->Children.empty()) {
            _currentTreeNode->NodeType = TreeNodeType::LeafNode;
            nodeTypeStr = "leaf";
        } else {
            _currentTreeNode->NodeType = TreeNodeType::InternalNode;
            nodeTypeStr = "internal";
        }

        _gstRoundNum = 0;

        std::string parentStr = _currentTreeNode->Parent ?
                                std::to_string(_currentTreeNode->Parent->PartitionId) : "null";
        std::string childrenStr;
        for (unsigned int i = 0; i < _currentTreeNode->Children.size(); ++i) {
            childrenStr += std::to_string(_currentTreeNode->Children[i]->PartitionId) + " ";
        }
        SLOG("I'm a/an " + nodeTypeStr + " node. Parent is " + parentStr + ". Children are " +
             childrenStr);
    }


    void Coordinator::SendGSTAtRoot() {
        // wait until all replicas are ready
        while (!_readyToServeRequests) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        while (true) {
            std::this_thread::sleep_for(std::chrono::microseconds(SysConfig::GSTComputationInterval));
            Exclusive lock(mtx);

#ifdef MEASURE_STATISTICS
            SysStats::NumRecvGSTs += 1;
#endif

            if (_numPartitions == 1) {
#ifdef DEP_VECTORS
                MVKVStore::Instance()->SetGSV(MVKVStore::Instance()->updateAndGetPVV());
#else
                MVKVStore::Instance()->SetGST(MVKVStore::Instance()->GetLST());
#endif
            } else {
#ifdef DEP_VECTORS
                std::vector<PhysicalTimeSpec> gsv = MVKVStore::Instance()->GetGSV();
                for (unsigned int i = 0; i < _currentTreeNode->Children.size(); ++i) {
                    _currentTreeNode->Children[i]->PartitionClient->SendGSV(gsv);
                }
#else
                PhysicalTimeSpec gst = MVKVStore::Instance()->GetGST();
                for (unsigned int i = 0; i < _currentTreeNode->Children.size(); ++i) {
                    _currentTreeNode->Children[i]->PartitionClient->SendGST(gst);
                }
#endif
            }
        }
    }

    void Coordinator::HandleGSTFromParent(PhysicalTimeSpec gst) {
        MVKVStore::Instance()->SetGSTIfSmaller(gst);

#ifdef MEASURE_STATISTICS
        SysStats::NumRecvGSTs += 1;
#endif

        if (_currentTreeNode->NodeType == TreeNodeType::InternalNode) {
            // send GST to children
            for (unsigned int i = 0; i < _currentTreeNode->Children.size(); ++i) {
                _currentTreeNode->Children[i]->PartitionClient->SendGST(gst);
            }
        } else if (_currentTreeNode->NodeType == TreeNodeType::LeafNode) {
            // start another round of GST computation
            _gstRoundNum += 1;

#ifdef MEASURE_STATISTICS
            SysStats::NumGSTRounds += 1;
#endif
            // send LST to parent
            PhysicalTimeSpec lst = MVKVStore::Instance()->GetLST();
            int round = _gstRoundNum;

            _currentTreeNode->Parent->PartitionClient->SendLST(lst, round);
        }
    }

    void Coordinator::HandleLSTFromChildren(PhysicalTimeSpec lst, int round) {
        {
            std::lock_guard <std::mutex> lk(_minLSTMutex);

            if (_receivedLSTCounts.find(round) == _receivedLSTCounts.end()) {
                _receivedLSTCounts[round] = 1;
                _minLSTs[round] = lst;
            } else {
                _receivedLSTCounts[round] += 1;
                _minLSTs[round] = min(_minLSTs[round], lst);
            }

            while (true) {
                int currentRound = _gstRoundNum + 1;

                if (_receivedLSTCounts.find(currentRound) != _receivedLSTCounts.end()) {
                    unsigned int lstCount = _receivedLSTCounts[currentRound];
                    PhysicalTimeSpec minLST = _minLSTs[currentRound];

                    if (lstCount == _currentTreeNode->Children.size()) {
                        PhysicalTimeSpec myLST = MVKVStore::Instance()->GetLST();
                        minLST = min(minLST, myLST);

                        // received all LSTs from children
                        if (_currentTreeNode->NodeType == TreeNodeType::InternalNode) {
                            // send LST to parent
                            _currentTreeNode->Parent->PartitionClient->SendLST(minLST, currentRound);
                        } else if (_currentTreeNode->NodeType == TreeNodeType::RootNode) {
                            PhysicalTimeSpec gst = minLST;
                            // update GST at root
                            MVKVStore::Instance()->SetGSTIfSmaller(gst);
                        }

                        _receivedLSTCounts.erase(currentRound);
                        _minLSTs.erase(currentRound);
                        _gstRoundNum = currentRound;
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
        }
    }

#ifdef DEP_VECTORS

    void Coordinator::HandleGSVFromParent(std::vector<PhysicalTimeSpec> gsv) {
        MVKVStore::Instance()->SetGSVIfSmaller(gsv);

#ifdef MEASURE_STATISTICS
        SysStats::NumRecvGSTs += 1;
#endif

        if (_currentTreeNode->NodeType == TreeNodeType::InternalNode) {
            // send GST to children
            for (unsigned int i = 0; i < _currentTreeNode->Children.size(); ++i) {
                _currentTreeNode->Children[i]->PartitionClient->SendGSV(gsv);
            }
        } else if (_currentTreeNode->NodeType == TreeNodeType::LeafNode) {
            // start another round of GST computation

            _gstRoundNum += 1;

#ifdef MEASURE_STATISTICS
            SysStats::NumGSTRounds += 1;
#endif
            // send LST to parent
            std::vector<PhysicalTimeSpec> pvv = MVKVStore::Instance()->updateAndGetPVV();
            int round = _gstRoundNum;

            _currentTreeNode->Parent->PartitionClient->SendPVV(pvv, round);
        }
    }

    void Coordinator::HandlePVVFromChildren(std::vector<PhysicalTimeSpec> pvv, int round) {
        {
            std::lock_guard<std::mutex> lk(_minLSTMutex);

            if (_receivedLSTCounts.find(round) == _receivedLSTCounts.end()) {
                _receivedLSTCounts[round] = 1;
                _minPVVs[round] = pvv;
            } else {
                _receivedLSTCounts[round] += 1;
                for (int i = 0; i < pvv.size(); i++) {
                    if (_minPVVs[round][i] > pvv[i])
                        _minPVVs[round][i] = pvv[i];
                }
            }

            while (true) {
                int currentRound = _gstRoundNum + 1;

                if (_receivedLSTCounts.find(currentRound) != _receivedLSTCounts.end()) {
                    unsigned int lstCount = _receivedLSTCounts[currentRound];
                    std::vector<PhysicalTimeSpec> minPVV = _minPVVs[currentRound];

                    if (lstCount == _currentTreeNode->Children.size()) {
                        std::vector<PhysicalTimeSpec> myPVV = MVKVStore::Instance()->updateAndGetPVV();
                        for (int i = 0; i < myPVV.size(); i++) {
                            if (minPVV[i] > myPVV[i])
                                minPVV[i] = myPVV[i];
                        }

                        // received all LSTs from children
                        if (_currentTreeNode->NodeType == TreeNodeType::InternalNode) {
                            // send LST to parent
                            _currentTreeNode->Parent->PartitionClient->SendPVV(minPVV, currentRound);
                        } else if (_currentTreeNode->NodeType == TreeNodeType::RootNode) {
                            std::vector<PhysicalTimeSpec> gsv = minPVV;
                            // update GST at root
                            MVKVStore::Instance()->SetGSVIfSmaller(gsv);
                        }

                        _receivedLSTCounts.erase(currentRound);
                        _minPVVs.erase(currentRound);
                        _gstRoundNum = currentRound;
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
        }
    }

#endif

#ifdef PARALLEL_XACTS

    void
    Coordinator::C_SendParallelInternalTxSliceGetResult(PbRpcKVInternalTxSliceGetResult &opResult, int partitionId) {
        _partitionClients[partitionId]->SendParallelInternalTxSliceGetResult(opResult);
    }

    ParallelXact *Coordinator::GetParallelXact(unsigned long id) {
        ParallelXact *ret;
        LOCK_XACT();
            ret = _xacts_ptrs[id];
        UNLOCK_XACT();
        assert(ret != NULL);
        return ret;
    }

#endif

} // namespace scc

