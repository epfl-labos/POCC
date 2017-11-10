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


#include "rpc/sync_rpc_client.h"
#include <unistd.h>
namespace scc
{

    SyncRPCClient::SyncRPCClient(std::string host, int port)
            : AbstractRPCClient(),
              _msgChannel(new MessageChannel(host, port)),
              _host(host),
              _port(port),
              invalid(false)
    {

    }

    SyncRPCClient::SyncRPCClient(std::string host, int port, bool invalid)
            : AbstractRPCClient(),
//_msgChannel(new MessageChannel(host, port)),
              _host(host),
              _port(port),
              invalid(invalid)
    {
      if (!invalid)
      {
        try
        {
          _msgChannel = std::make_shared<MessageChannel>(host, port);
        }
        catch (SocketException& e)
        {
          fprintf(stdout, "Exception in message channel ...\n");
          fflush(stdout);
        }
      }

    }

    void SyncRPCClient::Call(RPCMethod rid, std::string& args, std::string& results)
    {
      _Call(rid, args, results, true);
    }

    void SyncRPCClient::Call(RPCMethod rid, std::string& args)
    {
      std::string results;
      _Call(rid, args, results, false);
    }

    void SyncRPCClient::_Call(RPCMethod rid, std::string& args,
                              std::string& results, bool recvResults)
    {

      if (invalid)
      {
        throw SocketException("INVALID client!", false);
      }

      try
      {
        PbRpcRequest request;


        request.set_msgid(GetMessageId());
        request.set_methodid(static_cast<int32_t> (rid));
        request.set_arguments(args);

        _msgChannel->Send(request);

        if (recvResults)
        {

          // receive reply
          PbRpcReply reply;
          _msgChannel->Recv(reply);

          results = reply.results();
        }

      }
      catch (exception& e)
      {
        // invalid = true;
        fprintf(stdout, "[CALL FAILURE] %s : %s\n", getTextForEnum(rid), e.what());
        fflush(stdout);

        while (true)
        {
          try
          {
            fprintf(stdout, "Trying to create new MessageChannel\n");
            fflush(stdout);

            MessageChannelPtr chptr(new MessageChannel(_host, _port));
            _msgChannel = chptr;

            fprintf(stdout, "New MessageChannel created\n");
            fflush(stdout);
            break;
          }
          catch (SocketException& e)
          {
            fprintf(stdout, "[RECONNECTING] %s : unknown\n", getTextForEnum(rid));
            fflush(stdout);
            sleep(1);
          }
        }

        throw SocketException("RPC Call failure!", false);
      }
      catch (...)
      {
        invalid = true;
        fprintf(stdout, "[UNKNOWN CALL FAILURE] %s : unknown\n", getTextForEnum(rid));
        fflush(stdout);
        throw SocketException("RPC Call failure!", false);
      }

    }

} // namespace scc
/*
 namespace scc {

  SyncRPCClient::SyncRPCClient(std::string host, int port)
  : AbstractRPCClient(),
  _msgChannel(new MessageChannel(host, port)) { }

  void SyncRPCClient::Call(RPCMethod rid, std::string& args, std::string& results) {
    _Call(rid, args, results, true);
  }

  void SyncRPCClient::Call(RPCMethod rid, std::string& args) {
    std::string results;
    _Call(rid, args, results, false);
  }

  void SyncRPCClient::_Call(RPCMethod rid, std::string& args,
    std::string& results, bool recvResults) {
    PbRpcRequest request;

    request.set_msgid(GetMessageId());
    request.set_methodid(static_cast<int32_t> (rid));
    request.set_arguments(args);

    // send request 
    _msgChannel->Send(request);

    if (recvResults) {
      // receive reply
      PbRpcReply reply;
      _msgChannel->Recv(reply);

      results = reply.results();
    }
  }

} // namespace scc
 */
