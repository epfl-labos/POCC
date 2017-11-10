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


#include "rpc/message_channel.h"

namespace scc {

  MessageChannel::MessageChannel(std::string host, unsigned short port)
  : _host(host),
  _port(port),
  _socket(NULL) {

          // create a socket and connect to the remote server
          _socket = new TCPSocket(host, port);

  }

  // A socket should not be used in multiple message channels.

  MessageChannel::MessageChannel(TCPSocket* socket)
  : _host(),
  _port(-1),
  _socket(socket) { }

  MessageChannel::~MessageChannel() {
    delete _socket;
  }

}
