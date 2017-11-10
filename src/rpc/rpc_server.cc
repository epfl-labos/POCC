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


#include "rpc/rpc_server.h"

namespace scc {

  RPCServer::RPCServer(TCPSocket* socket)
  : _mc(socket) { }

  bool RPCServer::RecvRequest(PbRpcRequest& request) {
    // receive request
    _mc.Recv(request);
    return true;
  }

  bool RPCServer::SendReply(PbRpcReply& reply) {
    // send reply
    _mc.Send(reply);
    return true;
  }

}
