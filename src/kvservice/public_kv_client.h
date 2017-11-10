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


#ifndef SCC_KVSERVICE_PUBLIC_KV_CLIENT_H
#define SCC_KVSERVICE_PUBLIC_KV_CLIENT_H

#include "common/types.h"
#include "rpc/sync_rpc_client.h"
#include "rpc/rpc_id.h"
#include <string>
#include <vector>
#include <unordered_set>

namespace scc {

    class PublicKVClient {
    public:
        PublicKVClient(std::string serverName, int serverPort);

        PublicKVClient(std::string serverName, int serverPort, int numCtxs);

        ~PublicKVClient();

        void Echo(const std::string &input, std::string &output);

        bool Get(const std::string &key, std::string &value);

        bool Set(const std::string &key, const std::string &value);

        bool TxGet(const std::vector<std::string> &keySet,
                   std::vector<std::string> &valueSet);

        bool ShowItem(const std::string &key, std::string &itemVersions);

        bool ShowDB(std::string &allItemVersions);

        bool ShowState(std::string &stateStr);

        bool DumpLatencyMeasurement(std::string &resultStr);

        bool isKeyHot(string key);

        int getNumHotBlockedKeys();

        void resetSession();

    private:
        std::string _serverName;
        int _serverPort;
        SyncRPCClient *_rpcClient;
        int _numPartitions;
        int _numReplicasPerPartition;
        ConsistencyMetadata _sessionData;
        int _replicaId;
        int totalNumKeyInKVStore;
        int numBlockedHotKeys;

    private:
        bool GetServerConfig();
    };

} // namespace scc

#endif
