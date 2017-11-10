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


#ifndef SCC_KVSERVICE_PARTITION_KV_CLIENT_H
#define SCC_KVSERVICE_PARTITION_KV_CLIENT_H
#include "common/types.h"
#include "rpc/async_rpc_client.h"
#include "rpc/sync_rpc_client.h"
#include <string>
#include <vector>

namespace scc {

    class PartitionKVClient {
    public:
        PartitionKVClient(std::string serverName, int serverPort);

        ~PartitionKVClient();

        bool Get(ConsistencyMetadata &cdata,
                 const std::string &key,
                 std::string &value);

        bool Set(ConsistencyMetadata &cdata,
                 const std::string &key,
                 const std::string &value);

        bool TxSliceGet(ConsistencyMetadata &cdata,
                        const std::string &key,
                        std::string &value);

        bool ShowItem(const std::string &key, std::string &itemVersions);

        void InitializePartitioning(DBPartition source); // done by a different connection
        void SendLST(PhysicalTimeSpec lst, int round);

        void SendGST(PhysicalTimeSpec gst);

        bool GetPess(ConsistencyMetadata &cdata, const std::string &key, std::string &value);
        bool GetOpt(ConsistencyMetadata &cdata, const std::string &key, std::string &value);

#ifdef PARALLEL_XACTS
        void ParallelTxSliceGet(ConsistencyMetadata &cdata,
                                       const std::string &key,
                                       std::string &value, unsigned long id, int src) ;
        void SendParallelInternalTxSliceGetResult(PbRpcKVInternalTxSliceGetResult &opResult);
#endif

#ifdef DEP_VECTORS
        void SendPVV(std::vector<PhysicalTimeSpec> pvv, int round);

        void SendGSV(std::vector<PhysicalTimeSpec> gsv);

#endif

    private:
        std::string _serverName;
        int _serverPort;
        AsyncRPCClient *_asyncRpcClient;
        SyncRPCClient *_syncRpcClient;
#ifdef PARALLEL_XACTS
        AsyncRPCClient *_xactRpcClient;
#endif
    };

}

#endif
