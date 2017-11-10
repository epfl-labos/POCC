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


#include "kvservice/public_kv_client.h"
#include "messages/rpc_messages.pb.h"
#include "common/sys_logger.h"
#include "common/utils.h"
#include "common/sys_config.h"
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <time.h>
#include <iostream>

namespace scc {

    PublicKVClient::PublicKVClient(std::string serverName, int publicPort)
            : _serverName(serverName),
              _serverPort(publicPort) {
        _rpcClient = new SyncRPCClient(_serverName, _serverPort);
        numBlockedHotKeys = 0;

        GetServerConfig();

#ifdef DEP_VECTORS
        _sessionData.NDV.resize(_numReplicasPerPartition);
        _sessionData.DV.resize(_numReplicasPerPartition);
        ASSERT(_sessionData.NDV.size() == _sessionData.DV.size());
        for (int i = 0; i < _numReplicasPerPartition; i++) {
            PhysicalTimeSpec p(0, 0);
            _sessionData.NDV[i]=p;
            _sessionData.DV[i]=p;
        }
#endif

    }

    PublicKVClient::~PublicKVClient() {
        delete _rpcClient;
    }

    bool PublicKVClient::GetServerConfig() {
        std::string serializedArg;
        PbRpcKVPublicGetServerConfigResult result;
        std::string serializedResult;

        // call server
        _rpcClient->Call(RPCMethod::GetServerConfig, serializedArg, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);

        if (!result.succeeded()) {
            return false;
        }

        _numPartitions = result.numpartitions();
        _numReplicasPerPartition = result.numreplicasperpartition();
        SysConfig::OptimisticMode = result.optimisticmode();
        return true;
    }

    void PublicKVClient::Echo(const std::string &input, std::string &output) {
        assert(false);//Not using

        PbRpcEchoTest arg;
        std::string serializedArg;
        PbRpcEchoTest result;
        std::string serializedResult;

        // prepare argument
        arg.set_text(input);
        serializedArg = arg.SerializeAsString();

        // call server
        _rpcClient->Call(RPCMethod::EchoTest, serializedArg, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);
        output = result.text();
    }

    bool PublicKVClient::Get(const std::string &key, std::string &value) {
        assert(false);
        return true;
//        PbRpcKVPublicGetArg args;
//        std::string serializedArgs;
//        PbRpcKVPublicGetResult result;
//        std::string serializedResult;
//
//        // prepare arguments
//        args.set_key(key);
//
//        if (SysConfig::OptimisticMode) {
//
//#ifdef DEP_VECTORS
//            for (int j = 0; j < _numReplicasPerPartition; j++) {
//                PbPhysicalTimeSpec *vv = args.add_ndv();
//                vv->set_seconds(_sessionData.NDV[j].Seconds);
//                vv->set_nanoseconds(_sessionData.NDV[j].NanoSeconds);
//            }
//#else
//            // attach minLST
//            args.mutable_minlst()->set_seconds(_sessionData.minLST.Seconds);
//            args.mutable_minlst()->set_nanoseconds(_sessionData.minLST.NanoSeconds);
//#endif
//        } else {
//            // attach GST
//            args.mutable_gst()->set_seconds(_sessionData.GST.Seconds);
//            args.mutable_gst()->set_nanoseconds(_sessionData.GST.NanoSeconds);
//        }
//        // serialize arguments
//        serializedArgs = args.SerializeAsString();
//
//        // call server
//        _rpcClient->Call(RPCMethod::Get, serializedArgs, serializedResult);
//
//        // parse result
//        result.ParseFromString(serializedResult);
//        if (result.succeeded()) {
//            value = result.getvalue();
//
//            // extract DT and GST
//            PhysicalTimeSpec dt;
//
//            dt.Seconds = result.dt().seconds();
//            dt.NanoSeconds = result.dt().nanoseconds();
//
//            // minLST for optimistic mode and GST for not optimistic mode
//            if (SysConfig::OptimisticMode) {
//#ifdef DEP_VECTORS
//                PhysicalTimeSpec dt;
//                dt.Seconds = result.dt().seconds();
//                dt.NanoSeconds = result.dt().nanoseconds();
//                _sessionData.DV[result.srcreplica()] = MAX(_sessionData.DV[result.srcreplica()], dt);
//
//                for (int i = 0; i < result.dv_size(); i++) {
//                    const PbPhysicalTimeSpec &vv = result.dv(i);
//                    PhysicalTimeSpec p(vv.seconds(), vv.nanoseconds());
//                    _sessionData.NDV[i] = MAX(p, _sessionData.NDV[i]);
//                }
//#else
//                _sessionData.DT = MAX(_sessionData.DT, dt);
//
//                PhysicalTimeSpec dut;
//                dut.Seconds = result.dut().seconds();
//                dut.NanoSeconds = result.dut().nanoseconds();
//                _sessionData.minLST = MAX(_sessionData.minLST, dut);
//#endif//DEP_VECTORS
//            } else { //PESSIMISTIC
//                PhysicalTimeSpec gst;
//                gst.Seconds = result.gst().seconds();
//                gst.NanoSeconds = result.gst().nanoseconds();
//                _sessionData.GST = MAX(_sessionData.GST, gst);
//            }
//        }
//
//        return result.succeeded();
    }

    bool PublicKVClient::Set(const std::string &key, const std::string &value) {
        assert(false);
        return true;
//        PbRpcKVPublicSetArg args;
//        std::string serializedArgs;
//        PbRpcKVPublicSetResult result;
//        std::string serializedResult;
//
//        // prepare arguments
//        args.set_key(key);
//        args.set_value(value);
//
//
//#ifdef DEP_VECTORS
//        for (int j = 0; j < _numReplicasPerPartition; j++) {
//            PbPhysicalTimeSpec *vv = args.add_dv();
//            //Entry corresponding to local DC is set with PUT if PUT > minLSV[_replicaID]
//            vv->set_seconds(_sessionData.DV[j].Seconds);
//            vv->set_nanoseconds(_sessionData.DV[j].NanoSeconds);
//        }
//#else
//        // attach DT
//        args.mutable_dt()->set_seconds(_sessionData.DT.Seconds);
//        args.mutable_dt()->set_nanoseconds(_sessionData.DT.NanoSeconds);
//#endif
//
//        serializedArgs = args.SerializeAsString();
//
//        // call server
//        _rpcClient->Call(RPCMethod::Set, serializedArgs, serializedResult);
//
//        // parse result
//        result.ParseFromString(serializedResult);
//        if (result.succeeded()) {
//            // extract DT
//            PhysicalTimeSpec dt;
//            dt.Seconds = result.dt().seconds();
//            dt.NanoSeconds = result.dt().nanoseconds();
//
//#ifdef DEP_VECTORS
//
//            _sessionData.DV[result.srcreplica()].NanoSeconds = dt.NanoSeconds;
//            _sessionData.DV[result.srcreplica()].Seconds= dt.Seconds;
//
//#else
//            _sessionData.DT = MAX(_sessionData.DT, dt);
//
//#endif
//        }
//        return result.succeeded();
    }


    bool PublicKVClient::isKeyHot(std::string keyStr) {
        int key = atoi(keyStr.c_str());
        return key < (totalNumKeyInKVStore / 100);
    }

    bool PublicKVClient::TxGet(const std::vector <std::string> &keySet,
                               std::vector <std::string> &valueSet) {

        assert(false);
//        PbRpcKVPublicTxGetArg args;
//        std::string serializedArgs;
//        PbRpcKVPublicTxGetResult result;
//        std::string serializedResult;
//
//        // prepare arguments
//        for (unsigned int i = 0; i < keySet.size(); ++i) {
//            args.add_key(keySet[i]);
//        }
//
//        if (SysConfig::OptimisticMode) {
//            // attach minLST
////            args.mutable_minlst()->set_seconds(_sessionData.minLST.Seconds);
////            args.mutable_minlst()->set_nanoseconds(_sessionData.minLST.NanoSeconds);
//            args.mutable_dt()->set_seconds(_sessionData.DT.Seconds);
//            args.mutable_dt()->set_nanoseconds(_sessionData.DT.NanoSeconds);
//        } else {
//            // attach GST
//            args.mutable_gst()->set_seconds(_sessionData.GST.Seconds);
//            args.mutable_gst()->set_nanoseconds(_sessionData.GST.NanoSeconds);
//        }
//
//        // serialize arguments
//        serializedArgs = args.SerializeAsString();
//
//        // call server
//        _rpcClient->Call(RPCMethod::TxGet, serializedArgs, serializedResult);
//
//        // parse result
//        result.ParseFromString(serializedResult);
//        if (result.succeeded()) {
//            for (int i = 0; i < result.getvalue_size(); ++i) {
//                valueSet.push_back(result.getvalue(i));
//            }
//
//            // extract DT and GST
//
//            PhysicalTimeSpec dt;
//            dt.Seconds = result.dt().seconds();
//            dt.NanoSeconds = result.dt().nanoseconds();
//            _sessionData.DT = MAX(_sessionData.DT, dt);
//
//            if (!SysConfig::OptimisticMode) {
//
//                PhysicalTimeSpec gst;
//                gst.Seconds = result.gst().seconds();
//                gst.NanoSeconds = result.gst().nanoseconds();
//                _sessionData.GST = MAX(_sessionData.GST, gst);
//            } else {
//                PhysicalTimeSpec minLST;
//                minLST.Seconds = result.dut().seconds();
//                minLST.NanoSeconds = result.dut().nanoseconds();
//                _sessionData.minLST = MAX(_sessionData.minLST, minLST);
//            }
//        }
//
//        return result.succeeded();
        return true;
    }

    bool PublicKVClient::ShowItem(const std::string &key, std::string &itemVersions) {
        PbRpcKVPublicShowArg args;
        std::string serializedArgs;
        PbRpcKVPublicShowResult result;
        std::string serializedResult;

        // prepare arguments
        args.set_key(key);
        serializedArgs = args.SerializeAsString();

        // call server
        _rpcClient->Call(RPCMethod::ShowItem, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);
        if (result.succeeded()) {
            itemVersions = result.returnstring();
        }

        return result.succeeded();
    }

    bool PublicKVClient::ShowDB(std::string &allItemVersions) {
        std::string serializedArgs;
        PbRpcKVPublicShowResult result;
        std::string serializedResult;

        // call server
        _rpcClient->Call(RPCMethod::ShowDB, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);
        if (result.succeeded()) {
            allItemVersions = result.returnstring();
        }

        return result.succeeded();
    }

    bool PublicKVClient::ShowState(std::string &stateStr) {
        std::string serializedArgs;
        PbRpcKVPublicShowResult result;
        std::string serializedResult;

        // call server
        _rpcClient->Call(RPCMethod::ShowState, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);
        if (result.succeeded()) {
            stateStr = result.returnstring();
        }

        return result.succeeded();
    }

    bool PublicKVClient::DumpLatencyMeasurement(std::string &resultStr) {
        std::string serializedArgs;
        PbRpcKVPublicShowResult result;
        std::string serializedResult;

        // call server
        _rpcClient->Call(RPCMethod::DumpLatencyMeasurement, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);
        if (result.succeeded()) {
            resultStr = result.returnstring();
        }

        return result.succeeded();
    }

    void PublicKVClient::resetSession() {
        _sessionData.DT.setToZero();
        _sessionData.GST.setToZero();
        _sessionData.DUT.setToZero();
        _sessionData.minLST.setToZero();


#ifdef DEP_VECTORS
        for(int j=0;j<_numReplicasPerPartition;j++){
            _sessionData.DV[j].setToZero();
            _sessionData.NDV[j].setToZero();
        }
#endif //DEP_VECTORS
    }

} // namespace scc
