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


#ifndef SCC_KVSERVICE_PUBLIC_KV_CLIENT_LB_H
#define SCC_KVSERVICE_PUBLIC_KV_CLIENT_LB_H

#include "common/types.h"
#include "rpc/sync_rpc_client.h"
#include "rpc/rpc_id.h"
#include <string>
#include <vector>
#include <unordered_set>

namespace scc {

    class PublicClientLB {
    public:
        PublicClientLB(std::string serverName, int serverPort);

        PublicClientLB(std::string serverName, int serverPort, int numCtxs);

        ~PublicClientLB();

        void Echo(const std::string &input, std::string &output);

        bool Get(const std::string &key, std::string &value);

        bool Set(const std::string &key, const std::string &value);

        bool TxGet(const std::vector <std::string> &keySet, std::vector <std::string> &valueSet);

        void setTotalNumKeyInKVStore(int count);

        bool isKeyHot(string key);

        int getNumHotBlockedKeys();

        void resetSession();

        int getLocalReplicaId();

        int getNumReplicas();

    private:
        std::string _serverName;
        int _serverPort;
        SyncRPCClient *_rpcClient;
        int _numPartitions;
        int _numReplicasPerPartition;
        int totalNumKeyInKVStore;
        int numBlockedHotKeys;
        int _replicaId;
        ConsistencyMetadata _sessionData;

        bool GetOpt(const std::string &key, std::string &value);
        bool GetPess(const std::string &key, std::string &value);
        bool SetOpt(const std::string &key, const std::string &value);
        bool SetPess(const std::string &key, const std::string &value);

        bool TxGetOpt(const std::vector <std::string> &keySet, std::vector <std::string> &valueSet);
        bool TxGetPess(const std::vector <std::string> &keySet, std::vector <std::string> &valueSet);

    private:
        bool GetServerConfig();


    };

} // namespace scc

#endif
