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


#ifndef SCC_RPC_SYNC_RPC_CLIENT_H
#define SCC_RPC_SYNC_RPC_CLIENT_H

#include "rpc/abstract_rpc_client.h"
#include "rpc/message_channel.h"
#include "messages/rpc_messages.pb.h"
#include "common/exceptions.h"
#include <string>
#include <boost/thread.hpp>

namespace scc {

    class SyncRPCClient : public AbstractRPCClient {
    public:
        SyncRPCClient(std::string host, int port);
        SyncRPCClient(std::string host, int port, bool invalid);
        void Call(RPCMethod rid, std::string &args, std::string &results);
        void Call(RPCMethod rid, std::string &args);

    private:
        MessageChannelPtr _msgChannel;
        std::string _host;
        int _port;
        volatile bool invalid;
        void _Call(RPCMethod rid, std::string &args, std::string &results, bool recvResults);
    };

}

#endif
