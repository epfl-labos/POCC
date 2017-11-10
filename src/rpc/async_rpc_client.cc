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


#include "rpc/async_rpc_client.h"
#include "common/utils.h"
#include "common/sys_logger.h"
#include <unistd.h>

namespace scc {

  AsyncRPCClient::AsyncRPCClient(std::string host, int port, int numChannels)
  : _numChannels(numChannels) {

    srand(Utils::GetThreadId());

    for (int i = 0; i < _numChannels; i++) {
      // create message channels
      MessageChannelPtr mc(new MessageChannel(host, port));
      _msgChannels.push_back(mc);

      // initialize data structures
      PendingCallMapPtr s(new PendingCallMap());
      _pendingCallSets.push_back(s);
      MutexPtr m(new std::mutex());
      _pendingCallSetMutexes.push_back(m);

      WaitHandle* readyEvent = new WaitHandle();
      _replyHandlingThreadReadyEvents.push_back(readyEvent);
    }

    for (int i = 0; i < _numChannels; i++) {
      // launch reply handling threads
      std::thread t(&AsyncRPCClient::HandleReplyMessage, this, i);
      t.detach();
    }

    // wait all threads initialized
    WaitHandle::WaitAll(_replyHandlingThreadReadyEvents);
    // signal all threads ready
    _allReplyHandlingThreadsReady.Set();
  }

  AsyncRPCClient::~AsyncRPCClient() {
    for (unsigned int i = 0; i < _replyHandlingThreadReadyEvents.size(); i++) {
      delete _replyHandlingThreadReadyEvents[i];
    }
  }

  void AsyncRPCClient::Call(RPCMethod rid, std::string& args, AsyncState& state) {
    PbRpcRequest request;

    int channelId = random() % _numChannels;

    int64_t messageId = GetMessageId();
    request.set_msgid(messageId);
    request.set_methodid(static_cast<int32_t> (rid));
    request.set_arguments(args);

    // add call state to the pending list
    //std::thread::id this_id = std::this_thread::get_id();

    {
      std::lock_guard<std::mutex> lk(*_pendingCallSetMutexes[channelId]);
      _pendingCallSets[channelId]->insert(PendingCallMap::value_type(messageId, &state));
    }

    //fprintf(stdout,"%llx Sending WITH state msgId %d on channel %d and state %p\n",this_id, messageId, channelId, &state);fflush(stdout);

    // send request 
    _msgChannels[channelId]->Send(request);
  }

    void AsyncRPCClient::CallWithNoState(RPCMethod rid, std::string& args) {
      PbRpcRequest request;

      int channelId = random() % _numChannels;

      int64_t messageId = GetMessageId(); //TODO: do we need a mesg id if we do not wait for a reply?
      request.set_msgid(messageId);
      request.set_methodid(static_cast<int32_t> (rid));
      request.set_arguments(args);
      //std::thread::id this_id = std::this_thread::get_id();

      //fprintf(stdout,"%llx Sending with no state msgId %d on channel %d\n",this_id,messageId, channelId);fflush(stdout);
      // send request
      _msgChannels[channelId]->Send(request);
    }

  void AsyncRPCClient::HandleReplyMessage(int channelId) {
    // we access MessageChannel from a different thread
    // use std::shared_ptr to safely release MessageChannel
    MessageChannelPtr msgChannel(_msgChannels[channelId]);
    PendingCallMapPtr _pendingCallSet = _pendingCallSets[channelId];
    MutexPtr _pendingCallSetMutex = _pendingCallSetMutexes[channelId];

    // I'm ready
    _replyHandlingThreadReadyEvents[channelId]->Set();

    // wait the others ready
    _allReplyHandlingThreadsReady.Wait();

    // XXX: should catch socket exception here
    while (true) {
      // exit if the associated object is not alive anymore
      if (msgChannel.use_count() == 1) {
        return;
      }
      // receive reply
      PbRpcReply reply;
      msgChannel->Recv(reply);
      //fprintf(stdout,"Receiving msgId %d on channel %d\n",reply.msgid(), channelId);
      // get and remove the call state
      AsyncState* state = NULL;
      {
        std::lock_guard<std::mutex> lk(*_pendingCallSetMutex);
        state = (*_pendingCallSet)[reply.msgid()];
        _pendingCallSet->erase(reply.msgid());
      }

      // notify call finished
      state->Results = reply.results();
      //fprintf(stdout,"Setting cond var %p for msgId %d on channel %d\n",state,reply.msgid(), channelId);fflush(stdout);
      state->FinishEvent.Set();
      //fprintf(stdout,"Set cond var %p for msgId %d on channel %d\n", state,reply.msgid(),channelId);fflush(stdout);
    }
  }

} // namespace scc

