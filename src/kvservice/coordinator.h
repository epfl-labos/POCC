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


#ifndef SCC_KVSERVICE_COORDINATOR_H
#define SCC_KVSERVICE_COORDINATOR_H

#include "common/types.h"
#include "common/sys_config.h"
#include "groupservice/group_client.h"
#include "kvservice/replication_kv_client.h"
#include "kvservice/partition_kv_client.h"
#include <string>
#include <vector>
#include <map>
#include "kvservice/parallel_xact.h"
#include <boost/thread.hpp>

#ifdef XACT_SPIN
#define LOCK_XACT()  do{\
                        while(!__sync_bool_compare_and_swap((volatile unsigned int *)&_xact_lock, 0, 1)){\
                                __asm __volatile("pause\n": : :"memory");\
                            }\
                    }while(0)

#define UNLOCK_XACT() *(volatile unsigned int *)&_xact_lock = 0;

#else

#define LOCK_XACT() {std::lock_guard <std::mutex> lk(_xacts_mutex);

#define UNLOCK_XACT() }
#endif
namespace scc {

    enum class TreeNodeType;

    class TreeNode;

    class Coordinator {
    public:
        Coordinator(std::string serverName, int publicPort, int totalNumKeys);

        Coordinator(std::string serverName,
                    int publicPort,
                    int partitionPort,
                    int replicationPort,
                    int partitionId,
                    int replicaId,
                    int totalNumKeys,
                    std::string groupServerName,
                    int groupServerPort);

        ~Coordinator();

        // initialization
        void Initialize();

        // key-value interfaces
        void Echo(const std::string &input, std::string &output);

        bool Add(const std::string &key, const std::string &value);

        bool Get(ConsistencyMetadata &cdata,
                 const std::string &key,
                 std::string &value);

        bool Set(ConsistencyMetadata &cdata,
                 const std::string &key,
                 const std::string &value);

        bool TxGet(ConsistencyMetadata &cdata,
                   const std::vector <std::string> &keySet,
                   std::vector <std::string> &valueSet);

        bool TxSliceGet(ConsistencyMetadata &cdata,
                        const std::string &key,
                        std::string &value);
        // debugging
        bool ShowItem(const std::string &key, std::string &itemVersions);

        bool ShowDB(std::string &allItemVersions);

        bool ShowState(std::string &stateStr);

        bool DumpLatencyMeasurement(std::string &resultStr);

        // replication
        bool HandlePropagatedUpdate(std::vector<PropagatedUpdate *> &updates);

        bool HandleHeartbeat(Heartbeat &hb, int srcReplica);

        // partition
        bool InternalGet(ConsistencyMetadata &cdata,
                         const std::string &key,
                         std::string &value);

        bool InternalSet(ConsistencyMetadata &cdata,
                         const std::string &key,
                         const std::string &value);

        bool InternalTxSliceGet(ConsistencyMetadata &cdata,
                                const std::string &key,
                                std::string &value);


#ifdef DEP_VECTORS
        void WaitOnTxSlice(ConsistencyMetadata &cdata);
        void PhysWaitOnTxSlice(ConsistencyMetadata &cdata);
                PhysicalTimeSpec UpdateGSVAndComputeTransactionVector(ConsistencyMetadata &cdata);
#endif
    public:

        int NumPartitions() {
            return _numPartitions;
        }

        int NumReplicasPerPartition() {
            return _numReplicasPerPartition;
        }

        int ReplicaId() {
            return _replicaId;
        }

        boost::shared_mutex mtx;
        typedef boost::shared_lock<boost::shared_mutex> Shared;
        typedef boost::unique_lock<boost::shared_mutex> Exclusive;

#ifdef PARALLEL_XACTS
      void C_SendParallelInternalTxSliceGetResult(PbRpcKVInternalTxSliceGetResult &opResult, int partitionId);
      ParallelXact* GetParallelXact(unsigned long id);

         bool ParallelTxGet(ConsistencyMetadata &cdata,
                            const std::vector <std::string> &keySet,
                            std::vector <std::string> &valueSet);

        bool ParallelOptTxGet(ConsistencyMetadata &cdata,
                           const std::vector <std::string> &keySet,
                           std::vector <std::string> &valueSet);
#endif
    private:
        DBPartition _currentPartition;
        int _partitionId;
        int _replicaId;
        int _totalNumPreloadedKeys;
        bool _isDistributed;
        GroupClient *_groupClient;
        std::vector <std::vector<DBPartition>> _allPartitions;
        std::vector <DBPartition> _myPartitions;
        std::vector <DBPartition> _myReplicas;
        int _numPartitions;
        int _numReplicasPerPartition;
        bool _readyToServeRequests;
#ifdef PARALLEL_XACTS
        unsigned long _xact_id;
        volatile unsigned int _xact_lock;
        std::mutex _xacts_mutex;
        std::unordered_map<unsigned long, ParallelXact*> _xacts_ptrs;
#endif
    private:
        std::unordered_map<int, ReplicationKVClient *> _replicationClients;
        std::unordered_map<int, PartitionKVClient *> _partitionClients;

        void PropagatePersistedUpdate();

    private:
        TreeNode *_treeRoot;
        TreeNode *_currentTreeNode; // current partition node in the tree
        PhysicalTimeSpec _delta;

        void BuildGSTTree();

        // GST round number
        int _gstRoundNum;
        int _recvGSTRoundNum;

        std::unordered_map<int, int> _receivedLSTCounts;
        std::unordered_map<int, PhysicalTimeSpec> _minLSTs;
#ifdef DEP_VECTORS
        std::unordered_map<int, std::vector<PhysicalTimeSpec>> _minPVVs;
#endif
        std::mutex _minLSTMutex;

        std::unordered_map<int, int> _receivedLSTCountsBC;
        std::unordered_map<int, PhysicalTimeSpec> _minLSTsBC;
        std::mutex _minLSTMutexBC;

        // send gst
        WaitHandle _sendGSTEvent;

        void SendGSTAtRoot();

    public:
        void HandleLSTFromChildren(PhysicalTimeSpec lst, int round);

        void HandleGSTFromParent(PhysicalTimeSpec gst);

#ifdef DEP_VECTORS
        void HandlePVVFromChildren(std::vector<PhysicalTimeSpec> pvv, int round);
        void HandleGSVFromParent(std::vector<PhysicalTimeSpec> gsv);
#endif
    };

    enum class TreeNodeType {
        RootNode = 1,
        InternalNode = 2,
        LeafNode = 3
    };

    class TreeNode {
    public:
        std::string Name;
        int PartitionId;
        TreeNodeType NodeType;
        PartitionKVClient *PartitionClient;

        TreeNode *Parent;
        std::vector<TreeNode *> Children;
    };

} // namespace scc

#endif
