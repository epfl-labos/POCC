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


#ifndef SCC_KVSTORE_MV_KVSTORE_H_
#define SCC_KVSTORE_MV_KVSTORE_H_

#include "kvstore/item_anchor.h"
#include "kvstore/item_version.h"
#include "kvservice/coordinator.h"
#include "common/sys_stats.h"
#include <vector>
#include <unordered_map>
#include <thread>
#include <queue>
#include <atomic>

namespace scc {

    typedef std::unordered_map<std::string, ItemAnchor *> ItemIndexTable;
    typedef std::pair<ItemIndexTable::iterator, bool> IITResult;

    class MVKVStore {
    public:
        static MVKVStore *Instance();

        MVKVStore();

        ~MVKVStore();

        void SetPartitionInfo(DBPartition p, int numPartitions, int numReplicasPerPartition, int totalNumPreloadedKeys);

        void Initialize();

        void GetSysClockTime(PhysicalTimeSpec &physicalTime, int64_t &logicalTime);

        bool Add(const std::string &key, const std::string &value);

        bool AddAgain(const std::string &key, const std::string &value);

        bool Get(ConsistencyMetadata &cdata,
                 const std::string &key,
                 std::string &value);

        bool Set(ConsistencyMetadata &cdata,
                 const std::string &key,
                 const std::string &value);

#ifdef PARALLEL_XACTS
        bool LocalTxSliceGet(ConsistencyMetadata &cdata, const string &key, ParallelXact &p_x, int index);
#endif

        void WaitOnPut(ConsistencyMetadata &cdata);

        bool TxSliceGet(ConsistencyMetadata &cdata,
                        const std::string &key,
                        std::string &value);

        bool OptTxSliceGetOld(ConsistencyMetadata &cdata,
                              const std::string &key,
                              std::string &value);

        bool ShowItem(const std::string &key, std::string &itemVersions);

        bool ShowDB(std::string &allItemVersions);

        bool ShowTime(std::string &stateStr);

        bool ShowState(std::string &state);

        bool DumpLatencyMeasurement(std::string &resultStr);

        int Size();

        void Reserve(int numItems);

        bool HandlePropagatedUpdate(std::vector<PropagatedUpdate *> &updates);

        bool HandleHeartbeat(Heartbeat &hb, int srcReplica);

        //Staleness time measurement
        std::vector<StalenessTimeMeasurement> stalenessStartTimeStatisticsArrayPerReplica;
        std::vector<std::mutex *> stalenessStartTimeStatisticsMutexPerReplica;
        std::mutex _avgStalenessTimesMutex;
        std::vector<double> avgStalenessTimes;
        int stalenessBorder;

        std::mutex _sysTimeMutex;
        std::mutex _pvvMutex[32];
        std::vector<PhysicalTimeSpec> _PVV; // physical version vector
        std::vector<WaitHandle *> _pvv_waitHandle;


        inline int getReplicaId() {
            return _replicaId;
        }

        inline int getPartitionId() {
            return _partitionId;
        }

        void InitializeStalenessMeasurements();

        PhysicalTimeSpec START_TIME;

    private:
        std::vector<ItemIndexTable *> _indexTables;
        std::vector<std::mutex *> _indexTableMutexes;
        int _partitionId;
        int _replicaId;
        int _numPartitions;
        int _numReplicasPerPartition;
        int _totalNumPreloadedKeys;

        // system clock
        int64_t _localClock;
        int64_t _replicaClock;

        std::vector<int64_t> _LVV; // logical version vector (for debugging)
        PhysicalTimeSpec _initItemVersionPUT; // PUT of initially loaded item version

    private:
        PhysicalTimeSpec _GST;
#ifdef DEP_VECTORS
        std::vector<PhysicalTimeSpec> _GSV;
#endif
        std::mutex _GSTMutex;
        double epoch;
    public:
        PhysicalTimeSpec GetLST();

        PhysicalTimeSpec GetGST();

        void SetGST(PhysicalTimeSpec gst);

        void SetGSTIfSmaller(PhysicalTimeSpec gst);

        PhysicalTimeSpec GetAndUpdateLSTIfNeeded(PhysicalTimeSpec &target);

        void UpdateVVIfSmaller(PhysicalTimeSpec ts);

#ifdef DEP_VECTORS
        bool waitOnRead(ConsistencyMetadata &cdata, PhysicalTimeSpec &maxPVV);

        bool waitOnReadOnly(ConsistencyMetadata &cdata);

        bool waitAlsoOnThePut(ConsistencyMetadata &cdata);

        bool waitOnTxOnly(std::vector<PhysicalTimeSpec> &TV);

        std::vector<PhysicalTimeSpec> GetGSV();

        void SetGSVIfSmaller(std::vector<PhysicalTimeSpec> &gsv);

        void SetGSV(std::vector<PhysicalTimeSpec> gsv);

        std::vector<PhysicalTimeSpec> updateAndGetPVV();
        void updatePVV();

        std::vector<PhysicalTimeSpec> GetPVV();

        PhysicalTimeSpec GetPVV(int rid);

        volatile unsigned long pvv_counter;
#endif

#ifdef SPIN_LOCK //Use spinlocks
#error Spink locks do not perform well. Either they are poorly implemented or simply contention is too high to make them work
        unsigned long _gst_lock;
        unsigned long _pvv_lock;

#define PVV_LOCK() do{\
                        unsigned int wait;\
                        unsigned int i = 0;\
                        while (1){\
                            wait = 1;\
                            if (__sync_bool_compare_and_swap((volatile unsigned long *)&_pvv_lock, 0, 1))\
                                break;\
                            while(*(volatile unsigned long *)&_pvv_lock){\
                                for (i = 0; i < wait; i++)\
                                    __asm __volatile("pause\n": : :"memory");\
                                wait *= 2;\
                             }\
                        }\
                        (*(volatile unsigned long *) &pvv_counter)++;
                     }while(0)

#define PVV_UNLOCK()  (*(volatile unsigned long *) &pvv_counter)++; __asm __volatile("": : :"memory"); (*(volatile uint32_t *)&_pvv_lock) = 0;

#define GSV_LOCK() while (1){\
            if (__sync_bool_compare_and_swap((volatile unsigned long *)&_gsv_lock, 0, 1))\
                break;\
            while(*(volatile unsigned long *)&_pvv_lock){\
                    __asm __volatile("pause\n": : :"memory");\
            }\
        }

#define GSV_UNLOCK()  __asm __volatile("": : :"memory"); (*(volatile uint32_t *)&_gsv_lock) = 0

#else   //Use locks

#define PVV_LOCK(A)  { std::lock_guard <std::mutex> lk(_pvvMutex[A]);
#define PVV_ALL_LOCK()  {   for(int i=0;i<32;i++) std::lock_guard <std::mutex> lk(_pvvMutex[i]);

#define GSV_LOCK() { std::lock_guard <std::mutex> lk(_GSTMutex);

#define PVV_UNLOCK(A)  }
#define PVV_ALL_UNLOCK()  }

#define GSV_UNLOCK()  }

#endif

        bool AdvanceLocalPVV();

        PhysicalTimeSpec GetAndUpdateLST();

    private:
        void ShowVisibilityLatency(VisibilityStatistics &v);

        // update propagation
        void InstallPropagatedUpdate(PropagatedUpdate *update);

        // measure replication visibility latency
        std::mutex _pendingInvisibleVersionsMutex;
        std::vector<std::queue<ItemVersion *>> _pendingInvisibleVersions;
        std::vector<ReplicationLatencyResult> _recordedLatencies;
        int _numCheckedVersions;

        void CheckAndRecordVisibilityLatency();

        void calculateStalenessStatistics(StalenessStatistics &stat);

        void calculateUnmergedVersionsStatistics(UnmergedVersionsStatistics &stat);

        void addStalenessTimesToOutput(string &str);

        void calculateBlockStatistics(OptVersionBlockStatistics &bstat);

#ifdef DEP_VECTORS
        bool GetPessVec(ConsistencyMetadata &cdata, const std::string &key, std::string &value, ItemAnchor *itemAnchor);

        bool GetOptVec(ConsistencyMetadata &cdata, const std::string &key, std::string &value, ItemAnchor *itemAnchor);

#else
        bool
        GetPessScal(ConsistencyMetadata &cdata, const std::string &key, std::string &value, ItemAnchor *itemAnchor);
#endif

        void CreateBlockDurationPercentileStats(BlockDurationPercentilesStatistics &v);

    };

} // namespace scc

#endif
