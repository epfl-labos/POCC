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


#ifndef SCC_RPC_MESSAGE_CHANNEL_H
#define SCC_RPC_MESSAGE_CHANNEL_H

#include "rpc/socket.h"
#include <string>
#include <memory>
#include <boost/utility.hpp>
#include <thread>
#include <mutex>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <chrono>

using namespace std::chrono;

namespace scc
{

    typedef struct
    {
        int32_t len;
        uint8_t* buf;
        bool onheap;
    } MessageBuffer;

    class MessageChannel : boost::noncopyable
    {
    public:
        MessageChannel(std::string host, unsigned short port);
        MessageChannel(TCPSocket* socket);
        ~MessageChannel();
        template<class T> void Send(T& rpcMsg);
        template<class T> void Recv(T& rpcMsg);
        // message length takes 4 bytes (32bit integer)
        static const int NumMsgLenBytes = sizeof (int32_t);

        static long time_now()
        {
            system_clock::time_point tp = system_clock::now();
            system_clock::duration dtn = tp.time_since_epoch();
            return std::chrono::duration_cast<std::chrono::milliseconds>(dtn).count();
        }

    private:
        std::string _host;
        unsigned short _port;
        TCPSocket* _socket;
        std::mutex _sendMutex;
        std::mutex _recvMutex;
        static const int _MaxMessageArraySize = 16 * 1024;
    };

    typedef shared_ptr<MessageChannel> MessageChannelPtr;

    template<class T> void MessageChannel::Send(T& rpcMsg)
    {

        //    fprintf(stdout, "sadaaa %d BRM\n", (void*) &_socket);
        //    fflush(stdout);
        //    sleep(0.5);

        int32_t msgLen = rpcMsg.ByteSize();

        MessageBuffer sendBuf;

        sendBuf.len = 2 * NumMsgLenBytes + msgLen;
        sendBuf.onheap = sendBuf.len >= _MaxMessageArraySize;

        uint8_t _buf[!sendBuf.onheap ? sendBuf.len : 1];

        if (sendBuf.onheap)
        {
            sendBuf.buf = new uint8_t[sendBuf.len];
        }
        else
        {
            sendBuf.buf = _buf;
        }

        // set message length
        *(reinterpret_cast<int32_t*> (sendBuf.buf)) = msgLen;
        *(reinterpret_cast<int32_t*> (sendBuf.buf + NumMsgLenBytes)) = msgLen;

        // serialize message
        uint8_t * pbuf = sendBuf.buf + NumMsgLenBytes + NumMsgLenBytes;
        rpcMsg.SerializeWithCachedSizesToArray(pbuf);

        //    fprintf(stdout, "dfffdd %d BRM\n", (void*) &_socket);
        //    fflush(stdout);
        //    sleep(0.5);

        {
            std::lock_guard<std::mutex> lk(_sendMutex);

            //        fprintf(stdout, "mutex");
            //        fflush(stdout);
            //        sleep(0.5);

            long before = MessageChannel::time_now();
            long now = before;

            // send message length + message content
            int numSentBytes = 0;
            while ((numSentBytes < sendBuf.len) && (now - before < 1000))
            {
                //            fprintf(stdout, "while1");
                //            fflush(stdout);
                //            sleep(0.5);

                int nsent = _socket->send(sendBuf.buf + numSentBytes, sendBuf.len - numSentBytes);
                numSentBytes += nsent;

                //            fprintf(stdout, "while2");
                //            fflush(stdout);
                //            sleep(0.5);
                //            now = MessageChannel::time_now();
            }

            assert(numSentBytes == sendBuf.len);
            //        if (numSentBytes < sendBuf.len)
            //        {
            //            throw SocketException("Send timeout!", true);
            //        }
        }

        //    fprintf(stdout, "eee\n");
        //    fflush(stdout);
        //    sleep(0.5);

        // release message buffer if on heap
        if (sendBuf.onheap)
        {
            delete sendBuf.buf;
        }

        //    fprintf(stdout, "fff\n");
        //    fflush(stdout);
        //    sleep(0.5);
    }

    template<class T> void MessageChannel::Recv(T& rpcMsg)
    {
        //     fprintf(stdout, "msgChannel Recv ..\n");
        //                    fflush(stdout);
        MessageBuffer recvBuf;
        int32_t msgLen = 0;
        int32_t msgLen2 = 0;

        char* msgLenBuf = reinterpret_cast<char*> (&msgLen);
        char* msgLenBuf2 = reinterpret_cast<char*> (&msgLen2);

        {
            /*** FIRST LENGTH */

            std::lock_guard<std::mutex> lk(_recvMutex);

            int32_t recvLen = 0;
            long before = MessageChannel::time_now();
            long now = before;

            // read message length
            while ((recvLen < NumMsgLenBytes) && (now - before < 1000))
            {
                //        fprintf(stdout, "msgChannel WHILE 1..\n");
                //    fflush(stdout);
                int32_t len = _socket->recv(msgLenBuf + recvLen, NumMsgLenBytes - recvLen);
                recvLen += len;
                //        fprintf(stdout, "msgChannel WHILE 2..\n");
                //    fflush(stdout);
                now = MessageChannel::time_now();
            }

            assert(recvLen <= NumMsgLenBytes);
            if (recvLen < NumMsgLenBytes)
            {
                throw SocketException("Length receive timeout!", true);
            }
            assert(recvLen == NumMsgLenBytes);

            /*** SECOND LENGTH */

            recvLen = 0;
            before = MessageChannel::time_now();
            now = before;

            // read message length
            while ((recvLen < NumMsgLenBytes) && (now - before < 1000))
            {
                int32_t len = _socket->recv(msgLenBuf2 + recvLen, NumMsgLenBytes - recvLen);
                recvLen += len;
                now = MessageChannel::time_now();
            }

            assert(recvLen <= NumMsgLenBytes);
            if (recvLen < NumMsgLenBytes)
            {
                throw SocketException("Length receive timeout!", true);
            }
            assert(recvLen == NumMsgLenBytes);

            /** CHECK LENGTHS */

            if (msgLen != msgLen2)
            {
                //            fprintf(stdout, "LEN1 == %d, LEN2 == %d", msgLen, msgLen2);
                //            fflush(stdout);
                throw SocketException("The msg lengths differ!", true);
            }

            // allocate message buffer
            recvBuf.len = msgLen;
            uint8_t _buf[msgLen < _MaxMessageArraySize ? msgLen : 1];
            if (recvBuf.len < _MaxMessageArraySize)
            {
                recvBuf.buf = _buf;
                recvBuf.onheap = false;
            }
            else
            {
                recvBuf.buf = new uint8_t[msgLen];
                recvBuf.onheap = true;
            }

            // read message content
            recvLen = 0;
            before = MessageChannel::time_now();
            now = before;

            while ((recvLen < recvBuf.len) && (now - before < 1000))
            {
                int32_t len = _socket->recv(recvBuf.buf + recvLen, recvBuf.len - recvLen);
                recvLen += len;
                now = MessageChannel::time_now();
            }

            assert(recvLen <= recvBuf.len);
            if (recvLen < recvBuf.len)
            {
                throw SocketException("Data receive timeout!", true);
            }

            // deserialize message
            rpcMsg.ParseFromArray(recvBuf.buf, recvBuf.len);
        }

        // release message buffer if on heap
        if (recvBuf.onheap)
        {
            delete recvBuf.buf;
        }
    }

}

/*
#include "rpc/socket.h"
#include <string>
#include <memory>
#include <boost/utility.hpp>
#include <thread>
#include <mutex>

namespace scc {

    typedef struct {
        int32_t len;
        uint8_t* buf;
        bool onheap;
    } MessageBuffer;

    class MessageChannel : boost::noncopyable {
    public:
        MessageChannel(std::string host, unsigned short port);
        MessageChannel(TCPSocket* socket);
        ~MessageChannel();
        template<class T> void Send(T& rpcMsg);
        template<class T> void Recv(T& rpcMsg);
        // message length takes 4 bytes (32bit integer)
        static const int NumMsgLenBytes = sizeof (int32_t);
    private:
        std::string _host;
        unsigned short _port;
        TCPSocket* _socket;
        std::mutex _sendMutex;
        std::mutex _recvMutex;
        static const int _MaxMessageArraySize = 16 * 1024;
    };

    typedef shared_ptr<MessageChannel> MessageChannelPtr;

    template<class T> void MessageChannel::Send(T& rpcMsg) {
        int32_t msgLen = rpcMsg.ByteSize();
        MessageBuffer sendBuf;
        sendBuf.len = NumMsgLenBytes + msgLen;
        uint8_t _buf[sendBuf.len < _MaxMessageArraySize ? sendBuf.len : 1];
        if (sendBuf.len < _MaxMessageArraySize) {
            sendBuf.buf = _buf;
            sendBuf.onheap = false;
        } else {
            sendBuf.buf = new uint8_t[sendBuf.len];
            sendBuf.onheap = true;
        }

        // set message length
        *(reinterpret_cast<int32_t*> (sendBuf.buf)) = msgLen;

        // serialize message
        uint8_t * pbuf = sendBuf.buf + NumMsgLenBytes;
        rpcMsg.SerializeWithCachedSizesToArray(pbuf);


        {
            std::lock_guard<std::mutex> lk(_sendMutex);
            // send message length + message content
            int numSentBytes = 0;
            while (numSentBytes != sendBuf.len) {
                int nsent = _socket->send(sendBuf.buf + numSentBytes, sendBuf.len - numSentBytes);
                numSentBytes += nsent;
            }
        }

        // release message buffer if on heap
        if (sendBuf.onheap) {
            delete sendBuf.buf;
        }
    }

    template<class T> void MessageChannel::Recv(T& rpcMsg) {
        MessageBuffer recvBuf;
        int32_t msgLen = 0;
        char* msgLenBuf = reinterpret_cast<char*> (&msgLen);
        int32_t recvLen = 0;

        // read message length
        while (recvLen != NumMsgLenBytes) {
            int32_t len = _socket->recv(msgLenBuf + recvLen, NumMsgLenBytes - recvLen);
            recvLen += len;
        }

        // allocate message buffer
        recvBuf.len = msgLen;
        uint8_t _buf[msgLen < _MaxMessageArraySize ? msgLen : 1];
        if (recvBuf.len < _MaxMessageArraySize) {
            recvBuf.buf = _buf;
            recvBuf.onheap = false;
        } else {
            recvBuf.buf = new uint8_t[msgLen];
            recvBuf.onheap = true;
        }

        // read message content
        recvLen = 0;
        while (recvLen != recvBuf.len) {
            int32_t len = _socket->recv(recvBuf.buf + recvLen, recvBuf.len - recvLen);
            recvLen += len;
        }

        // deserialize message
        rpcMsg.ParseFromArray(recvBuf.buf, recvBuf.len);

        // release message buffer if on heap
        if (recvBuf.onheap) {
            delete recvBuf.buf;
        }
    }

}
 */

#endif
