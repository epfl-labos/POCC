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


#include "kvservice/public_kv_client_lb.h"
#include "messages/rpc_messages.pb.h"
#include "common/sys_logger.h"
#include "common/utils.h"
#include "common/sys_config.h"
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <time.h>
#include <iostream>

#ifndef USE_GSV_AS_DV
#error Tx only with USE_GSV_AS_DV
#endif

namespace scc {

    PublicClientLB::PublicClientLB(std::string serverName, int publicPort)
            : _serverName(serverName),
              _serverPort(publicPort) {

        try {
            _rpcClient = new SyncRPCClient(_serverName, _serverPort);
        }
        catch (...) {
            fprintf(stdout, "[ INVALID PUBLIC_LB CLIENT] %s\n", "...");
            fflush(stdout);
            bool stop = false;
            while (!stop) {
                try {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    _rpcClient = new SyncRPCClient(_serverName, _serverPort, true);
                    fprintf(stdout, "RECREATED  SyncRPCClient %s\n", "...");
                    fflush(stdout);
                    stop = true;
                }
                catch (...) {
                    fprintf(stdout, " FAILED Recreation of PUBLIC_LB CLIENT. Trying again  %s\n", "...");
                    fflush(stdout);
                }
            }
        }

        numBlockedHotKeys = 0;
        try {
            GetServerConfig();
        }
        catch (...) {
            bool stop = false;
            int retries = 20;
            while (!stop) {
                try {
                    retries--;
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    if (retries > 0) {
                        GetServerConfig();
                        stop = true;
                    } else {
                        retries = 20;
                        delete _rpcClient;
                        _rpcClient = new SyncRPCClient(_serverName, _serverPort);

                    }

                }
                catch (...) {

                    fprintf(stdout, " FAILED Retry of GetServerConfig. Trying again  %s\n", "...");
                    fflush(stdout);
                }
            }
        }

#ifdef DEP_VECTORS
        _sessionData.NDV.resize(_numReplicasPerPartition);
        _sessionData.DV.resize(_numReplicasPerPartition);
        _sessionData.GSV.resize(_numReplicasPerPartition);
        ASSERT(_sessionData.NDV.size() == _sessionData.DV.size());
        for (int i = 0; i < _numReplicasPerPartition; i++) {
            PhysicalTimeSpec p(0, 0);
            PhysicalTimeSpec p1(0, 0);
            PhysicalTimeSpec p2(0, 0);
            _sessionData.NDV[i] = p;
            _sessionData.DV[i] = p1;
            _sessionData.GSV[i] = p2;
        }
#endif

    }

    PublicClientLB::~PublicClientLB() {
        delete _rpcClient;
    }

    bool PublicClientLB::GetServerConfig() {
        std::string serializedArg;
        PbRpcKVPublicGetServerConfigResult result;
        std::string serializedResult;

        // call server
        _rpcClient->Call(RPCMethod::GetServerConfig, serializedArg, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);

        if (!result.succeeded()) {
            SLOG("Could not get server config");
            return false;
        }

        _numPartitions = result.numpartitions();
        _numReplicasPerPartition = result.numreplicasperpartition();

        _replicaId = result.replicaid();

        SysConfig::OptimisticMode = result.optimisticmode();

        return true;
    }

    int PublicClientLB::getLocalReplicaId() {
        return _replicaId;
    }

    int PublicClientLB::getNumReplicas() {
        return _numReplicasPerPartition;
    }

    void PublicClientLB::setTotalNumKeyInKVStore(int count) {
        totalNumKeyInKVStore = count;
    }

    void PublicClientLB::resetSession() {
        _sessionData.DT.setToZero();
        _sessionData.GST.setToZero();
        _sessionData.DUT.setToZero();
        _sessionData.minLST.setToZero();

#ifdef DEP_VECTORS
        for (int j = 0; j < _numReplicasPerPartition; j++) {
            _sessionData.DV[j].setToZero();
            _sessionData.NDV[j].setToZero();
            _sessionData.GSV[j].setToZero();
        }
#endif //DEP_VECTORS
    }

    void PublicClientLB::Echo(const std::string &input, std::string &output) {
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

    bool PublicClientLB::Get(const std::string &key, std::string &value) {

        if (SysConfig::OptimisticMode) {
            return GetOpt(key, value);
        }
        return GetPess(key, value);
    }

    bool PublicClientLB::Set(const std::string &key, const std::string &value) {
        if (SysConfig::OptimisticMode) {
            return SetOpt(key, value);
        }
        return SetPess(key, value);
    }

#ifdef DEP_VECTORS

    // #################################### VECTORS ######################################################

    bool PublicClientLB::GetOpt(const std::string &key, std::string &value) {
        PbRpcKVPublicGetArg args;
        std::string serializedArgs;
        PbRpcKVPublicGetResult result;
        std::string serializedResult;

        // prepare arguments
        args.set_key(key);

        for (int j = 0; j < _numReplicasPerPartition; j++) {
            PbPhysicalTimeSpec *vv = args.add_ndv();
            vv->set_seconds(_sessionData.NDV[j].Seconds);
            vv->set_nanoseconds(_sessionData.NDV[j].NanoSeconds);
        }

        serializedArgs = args.SerializeAsString();

        // call server
        _rpcClient->Call(RPCMethod::Get, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);

        if (result.succeeded()) {
            value = result.getvalue();

            int rid = result.srcreplica();

            PhysicalTimeSpec dt;

#ifndef EMBEDDED_DT_IN_SCRREP_DV

            dt.Seconds = result.dt().seconds();
            dt.NanoSeconds = result.dt().nanoseconds();
            _sessionData.DV[rid] = MAX(_sessionData.DV[rid], dt);
#endif

            for (int i = 0; i < _numReplicasPerPartition; i++) {
                const PbPhysicalTimeSpec &vv = result.dv(i);
                PhysicalTimeSpec p(vv.seconds(), vv.nanoseconds());
                _sessionData.NDV[i] = MAX(p, _sessionData.NDV[i]);

            }
        }
        return result.succeeded();
    }

    bool PublicClientLB::GetPess(const std::string &key, std::string &value) {
        PbRpcKVPublicGetArg args;
        std::string serializedArgs;
        PbRpcKVPublicGetResult result;
        std::string serializedResult;

        // prepare arguments
        args.set_key(key);

        for (int j = 0; j < _numReplicasPerPartition; j++) {
            PbPhysicalTimeSpec *vv = args.add_gsv();
            vv->set_seconds(_sessionData.GSV[j].Seconds);
            vv->set_nanoseconds(_sessionData.GSV[j].NanoSeconds);
        }

        serializedArgs = args.SerializeAsString();

        // call server
        _rpcClient->Call(RPCMethod::Get, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);

        if (result.succeeded()) {
            value = result.getvalue();
            int rid = result.srcreplica();

            PhysicalTimeSpec dt(result.dt().seconds(), result.dt().nanoseconds());
#ifndef EMBEDDED_DT_IN_SCRREP_DV
            _sessionData.DV[rid] = MAX(_sessionData.DV[rid], dt);
#else
            if(rid == _replicaId){
                 _sessionData.GSV[rid] = MAX(_sessionData.GSV[rid], dt);
            }
#endif //EMBEDDED_DT_IN_SCRREP_DV

            for (int i = 0; i < _numReplicasPerPartition; i++) {
                const PbPhysicalTimeSpec &vv = result.gsv(i);
                PhysicalTimeSpec p(vv.seconds(), vv.nanoseconds());
                _sessionData.GSV[i] = MAX(p, _sessionData.GSV[i]);
            }
        } else {
            SLOG("FAILURE!!!!");
        }

        return result.succeeded();
    }


    bool PublicClientLB::SetOpt(const std::string &key, const std::string &value) {
        PbRpcKVPublicSetArg args;
        std::string serializedArgs;
        PbRpcKVPublicSetResult result;
        std::string serializedResult;

        // prepare arguments
        args.set_key(key);
        args.set_value(value);
        for (int j = 0; j < _numReplicasPerPartition; j++) {

            PbPhysicalTimeSpec *vv = args.add_dv();
#ifndef  EMBEDDED_DT_IN_SCRREP_DV
            PhysicalTimeSpec p = MAX(_sessionData.DV[j], _sessionData.NDV[j]);
            vv->set_seconds(p.Seconds);
            vv->set_nanoseconds(p.NanoSeconds);
#else
            vv->set_seconds(_sessionData.NDV[j].Seconds);
            vv->set_nanoseconds(_sessionData.NDV[j].NanoSeconds);

#endif //EMBEDDED_DT_IN_SCRREP_DV
        }
        serializedArgs = args.SerializeAsString();

        // call server
        _rpcClient->Call(RPCMethod::Set, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);
        if (result.succeeded()) {
            PhysicalTimeSpec dt;
            dt.Seconds = result.dt().seconds();
            dt.NanoSeconds = result.dt().nanoseconds();
#ifndef  EMBEDDED_DT_IN_SCRREP_DV
            _sessionData.DV[_replicaId].NanoSeconds = dt.NanoSeconds;
            _sessionData.DV[_replicaId].Seconds = dt.Seconds;
#else
            _sessionData.NDV[_replicaId].NanoSeconds = dt.NanoSeconds;
            _sessionData.NDV[_replicaId].Seconds = dt.Seconds;
#endif //EMBEDDED_DT_IN_SCRREP_DV
        }
        return result.succeeded();
    }

    bool PublicClientLB::SetPess(const std::string &key, const std::string &value) {
        PbRpcKVPublicSetArg args;
        std::string serializedArgs;
        PbRpcKVPublicSetResult result;
        std::string serializedResult;

        // prepare arguments
        args.set_key(key);
        args.set_value(value);

        for (int j = 0; j < _numReplicasPerPartition; j++) {
            PbPhysicalTimeSpec *vv = args.add_dv();

#ifndef EMBEDDED_DT_IN_SCRREP_DV
            PhysicalTimeSpec p = MAX(_sessionData.DV[j], _sessionData.GSV[j]);
            vv->set_seconds(p.Seconds);
            vv->set_nanoseconds(p.NanoSeconds);

#else
            vv->set_seconds(_sessionData.GSV[j].Seconds);
            vv->set_nanoseconds(_sessionData.GSV[j].NanoSeconds);
#endif // EMBEDDED_DT_IN_SCRREP_DV

        }

        serializedArgs = args.SerializeAsString();

        // call server
        _rpcClient->Call(RPCMethod::Set, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);
        if (result.succeeded()) {
            PhysicalTimeSpec dt;
            dt.Seconds = result.dt().seconds();
            dt.NanoSeconds = result.dt().nanoseconds();

#ifndef EMBEDDED_DT_IN_SCRREP_DV
            _sessionData.DV[_replicaId].NanoSeconds = dt.NanoSeconds;
            _sessionData.DV[_replicaId].Seconds = dt.Seconds;
#else
            _sessionData.GSV[_replicaId].NanoSeconds = dt.NanoSeconds;
            _sessionData.GSV[_replicaId].Seconds = dt.Seconds;
#endif //EMBEDDED_DT_IN_SCRREP_DV

        } else {
            SLOG("SET result failure !!!!");
        }

        return result.succeeded();
    }

    bool PublicClientLB::TxGet(const std::vector<std::string> &keySet, std::vector<std::string> &valueSet) {
        PbRpcKVPublicTxGetArg args;
        std::string serializedArgs;
        PbRpcKVPublicTxGetResult result;
        std::string serializedResult;

        // prepare arguments
        for (unsigned int i = 0; i < keySet.size(); ++i) {
            args.add_key(keySet[i]);
        }

        for (int j = 0; j < _numReplicasPerPartition; j++) {
            PbPhysicalTimeSpec *vv = args.add_sv();

            if (SysConfig::OptimisticMode) {
                vv->set_seconds(_sessionData.NDV[j].Seconds);
                vv->set_nanoseconds(_sessionData.NDV[j].NanoSeconds);
            } else {
                vv->set_seconds(_sessionData.GSV[j].Seconds);
                vv->set_nanoseconds(_sessionData.GSV[j].NanoSeconds);
            }
        }

        args.mutable_st()->set_seconds(_sessionData.DV[_replicaId].Seconds);
        args.mutable_st()->set_nanoseconds(_sessionData.DV[_replicaId].NanoSeconds);

        // serialize arguments
        serializedArgs = args.SerializeAsString();

        // call server
        _rpcClient->Call(RPCMethod::TxGet, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);
        //fprintf(stdout,"%s\n",result.DebugString().c_str());fflush(stdout);

        if (result.succeeded()) {
            for (int i = 0; i < result.getvalue_size(); ++i) {
                valueSet.push_back(result.getvalue(i));
            }

            for (int i = 0; i < _numReplicasPerPartition; i++) {
                const PbPhysicalTimeSpec &vv = result.sv(i);
                PhysicalTimeSpec p(vv.seconds(), vv.nanoseconds());

                if (SysConfig::OptimisticMode) {
                    _sessionData.NDV[i] = MAX(p, _sessionData.NDV[i]);
                } else {
                    _sessionData.GSV[i] = MAX(p, _sessionData.GSV[i]);
                }
            }

            //These are the highest per-DC update times corresponding to read items
            for (int i = 0; i < _numReplicasPerPartition; i++) {
                const PbPhysicalTimeSpec &vv = result.uv(i);
                PhysicalTimeSpec p(vv.seconds(), vv.nanoseconds());
                _sessionData.DV[i] = MAX(p, _sessionData.DV[i]);
            }
        }
        return result.succeeded();
    }

#else  //SCALAR
    // #################################### SCALAR ######################################################

    bool PublicClientLB::GetPess(const std::string &key, std::string &value) {
        PbRpcKVPublicGetArg args;
        std::string serializedArgs;
        PbRpcKVPublicGetResult result;
        std::string serializedResult;

        // prepare arguments
        args.set_key(key);

        args.mutable_gst()->set_seconds(_sessionData.GST.Seconds);
        args.mutable_gst()->set_nanoseconds(_sessionData.GST.NanoSeconds);
        serializedArgs = args.SerializeAsString();

        // call server
        _rpcClient->Call(RPCMethod::Get, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);

        if (result.succeeded()) {
            value = result.getvalue();

            // extract DT and GST

            PhysicalTimeSpec dt;

            dt.Seconds = result.dt().seconds();
            dt.NanoSeconds = result.dt().nanoseconds();
            PhysicalTimeSpec gst;
            gst.Seconds = result.gst().seconds();
            gst.NanoSeconds = result.gst().nanoseconds();
            _sessionData.GST = MAX(_sessionData.GST, gst);
        }
        return result.succeeded();

    }

    bool PublicClientLB::SetPess(const std::string &key, const std::string &value) {
        PbRpcKVPublicSetArg args;
        std::string serializedArgs;
        PbRpcKVPublicSetResult result;
        std::string serializedResult;

        // prepare arguments
        args.set_key(key);
        args.set_value(value);

        args.mutable_dt()->set_seconds(_sessionData.DT.Seconds);
        args.mutable_dt()->set_nanoseconds(_sessionData.DT.NanoSeconds);
        serializedArgs = args.SerializeAsString();

        // call server
        _rpcClient->Call(RPCMethod::Set, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);
        if (result.succeeded()) {
            PhysicalTimeSpec dt;
            dt.Seconds = result.dt().seconds();
            dt.NanoSeconds = result.dt().nanoseconds();
            assert(dt > _sessionData.DT);
            _sessionData.DT = dt;
        }
        return result.succeeded();
    }

    bool PublicClientLB::TxGet(const std::vector <std::string> &keySet, std::vector <std::string> &valueSet) {
        if (SysConfig::OptimisticMode)
            return TxGetOpt(keySet, valueSet);
        return TxGetPess(keySet, valueSet);
    }

    bool PublicClientLB::TxGetOpt(const std::vector <std::string> &keySet, std::vector <std::string> &valueSet) {
        //Not supported
        assert(false);
    }

    bool PublicClientLB::TxGetPess(const std::vector <std::string> &keySet, std::vector <std::string> &valueSet) {
        PbRpcKVPublicTxGetArg args;
        std::string serializedArgs;
        PbRpcKVPublicTxGetResult result;
        std::string serializedResult;

        // prepare arguments
        for (unsigned int i = 0; i < keySet.size(); ++i) {
            args.add_key(keySet[i]);
        }

        args.mutable_ut()->set_seconds(_sessionData.DT.Seconds);
        args.mutable_ut()->set_nanoseconds(_sessionData.DT.NanoSeconds);
        args.mutable_st()->set_seconds(_sessionData.GST.Seconds);
        args.mutable_st()->set_nanoseconds(_sessionData.GST.NanoSeconds);

        // serialize arguments
        serializedArgs = args.SerializeAsString();

        // call server
        _rpcClient->Call(RPCMethod::TxGet, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);
        if (result.succeeded()) {
            for (int i = 0; i < result.getvalue_size(); ++i) {
                valueSet.push_back(result.getvalue(i));
            }
            PhysicalTimeSpec ut;
            ut.Seconds = result.ut().seconds();
            ut.NanoSeconds = result.ut().nanoseconds();
            _sessionData.DT = MAX(_sessionData.DT, ut);

            PhysicalTimeSpec st;
            st.Seconds = result.st().seconds();
            st.NanoSeconds = result.st().nanoseconds();
            _sessionData.GST = MAX(_sessionData.GST, st);
        }

        return result.succeeded();
    }

#endif //SCALAR

} // namespace scc
