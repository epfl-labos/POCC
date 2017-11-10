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


#ifndef SCC_KVSERVICE_KV_SERVER_H
#define SCC_KVSERVICE_KV_SERVER_H

#include "common/types.h"
#include "kvservice/coordinator.h"
#include "rpc/socket.h"
#include "rpc/rpc_server.h"
#include "messages/rpc_messages.pb.h"
#include "messages/op_log_entry.pb.h"
#include "common/sys_stats.h"
#include "kvservice/parallel_xact.h"
#include <gperftools/profiler.h>
#include <boost/thread.hpp>

namespace scc {

    class KVServer {
    public:
        KVServer(std::string name, unsigned short publicPort, int totalNumKeys);

        KVServer(std::string name,
                 unsigned short publicPort,
                 unsigned short partitionPort,
                 unsigned short replicationPort,
                 int partitionId,
                 int replicaId,
                 int totalNumKeys,
                 std::string groupServerName,
                 int groupServerPort);

        ~KVServer();

        void Run();
        void Profile();

    private:
        std::string _serverName;
        int _publicPort;
        int _partitionPort;
        int _replicationPort;
        int _replicaId;
        int _partitionId;
        Coordinator *_coordinator;

        typedef boost::shared_lock<boost::shared_mutex> Shared;
        typedef boost::unique_lock<boost::shared_mutex> Exclusive;

    private:
        void ServePublicConnection();
        void HandlePublicRequest(TCPSocket *clientSocket);
        void processPublicRequest(RPCServerPtr& rpcServer, PbRpcRequest& rpcRequest);

        void ServePartitionConnection();
        void HandlePartitionRequest(TCPSocket *partitionSocket);
        void processPartitionRequest(RPCServerPtr& rpcServer, PbRpcRequest& rpcRequest, DBPartition& servedPartition);

        void ServeReplicationConnection();
        void HandleReplicationRequest(TCPSocket *replicaSocket);
        void processReplicationRequest(RPCServerPtr& rpcServer, PbRpcRequest& rpcRequest, DBPartition& servedPartition);

        void HandleGetOpt(PbRpcKVPublicGetArg &opArg, PbRpcKVPublicGetResult &opResult);
        void HandleGetPess(PbRpcKVPublicGetArg &opArg, PbRpcKVPublicGetResult &opResult);

        void HandleInternalGetOpt(PbRpcKVInternalGetArg &opArg, PbRpcKVInternalGetResult &opResult);
        void HandleInternalGetPess(PbRpcKVInternalGetArg &opArg, PbRpcKVInternalGetResult &opResult);

        void HandleTxGet(PbRpcKVPublicTxGetArg &opArg, PbRpcKVPublicTxGetResult &opResult);
        void HandleInternalTxSliceGet(PbRpcKVInternalTxSliceGetArg &opArg,
                                                PbRpcKVInternalTxSliceGetResult &opResult);

    };

} // namespace scc

#endif
