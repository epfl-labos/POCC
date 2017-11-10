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


#include <common/utils.h>
#include "kvservice/partition_kv_client.h"
#include "common/sys_config.h"
#include "messages/rpc_messages.pb.h"
#include "common/sys_logger.h"
#include "common/sys_stats.h"
namespace scc {

    PartitionKVClient::PartitionKVClient(std::string serverName, int serverPort)
            : _serverName(serverName),
              _serverPort(serverPort),
              _asyncRpcClient(NULL),
              _syncRpcClient(NULL)
    {
        _asyncRpcClient = new AsyncRPCClient(serverName, serverPort,
                                             SysConfig::NumChannelsPartition);
        _syncRpcClient = new SyncRPCClient(serverName, serverPort);
    }

    PartitionKVClient::~PartitionKVClient() {
        delete _asyncRpcClient;
    }

#ifdef DEP_VECTORS
    bool PartitionKVClient::GetOpt(ConsistencyMetadata &cdata, const std::string &key, std::string &value) {
        PbRpcKVInternalGetArg opArg;
        PbRpcKVInternalGetResult opResult;
        AsyncState state;

        // prepare arguments
        opArg.set_key(key);
        for (int i = 0; i < cdata.NDV.size(); i++) {
            PbPhysicalTimeSpec *vv = opArg.add_ndv();
            vv->set_seconds(cdata.NDV[i].Seconds);
            vv->set_nanoseconds(cdata.NDV[i].NanoSeconds);
        }

        std::string serializedArg = opArg.SerializeAsString();

        // call server
        _asyncRpcClient->Call(RPCMethod::InternalGet, serializedArg, state);
        state.FinishEvent.WaitAndReset();
        // parse result
        opResult.ParseFromString(state.Results);
        if (opResult.succeeded()) {
            value = opResult.getvalue();

#ifndef EMBEDDED_DT_IN_SCRREP_DV
            cdata.DT.Seconds = opResult.dt().seconds();
            cdata.DT.NanoSeconds = opResult.dt().nanoseconds();
#endif

            for (int i = 0; i < opResult.dv_size(); i++) {
                const PbPhysicalTimeSpec &vv = opResult.dv(i);
                PhysicalTimeSpec p(vv.seconds(), vv.nanoseconds());
                cdata.DV.push_back(p);
            }

            cdata.SrcReplica = opResult.srcreplica();
        }
        return opResult.succeeded();
    }

    bool PartitionKVClient::GetPess(ConsistencyMetadata &cdata, const std::string &key, std::string &value) {
        PbRpcKVInternalGetArg opArg;
        PbRpcKVInternalGetResult opResult;
        AsyncState state;

        // prepare arguments
        opArg.set_key(key);
        for (int i = 0; i < cdata.GSV.size(); i++) {
            PbPhysicalTimeSpec *vv = opArg.add_gsv();
            vv->set_seconds(cdata.GSV[i].Seconds);
            vv->set_nanoseconds(cdata.GSV[i].NanoSeconds);
        }

        std::string serializedArg = opArg.SerializeAsString();

        // call server
        _asyncRpcClient->Call(RPCMethod::InternalGet, serializedArg, state);
        state.FinishEvent.WaitAndReset();
        // parse result
        opResult.ParseFromString(state.Results);
        if (opResult.succeeded()) {
            value = opResult.getvalue();

#ifndef EMBEDDED_DT_IN_SCRREP_DV
            cdata.DT.Seconds = opResult.dt().seconds();
            cdata.DT.NanoSeconds = opResult.dt().nanoseconds();
#endif

            for (int i = 0; i < opResult.gsv_size(); i++) {
                const PbPhysicalTimeSpec &vv = opResult.gsv(i);
                cdata.GSV[i].Seconds = vv.seconds();
                cdata.GSV[i].NanoSeconds = vv.nanoseconds();
            }

            cdata.SrcReplica = opResult.srcreplica();
        }
        return opResult.succeeded();
    }
#else

    bool PartitionKVClient::GetOpt(ConsistencyMetadata &cdata, const std::string &key, std::string &value) {
        PbRpcKVInternalGetArg opArg;
        PbRpcKVInternalGetResult opResult;
        AsyncState state;

        // prepare arguments
        opArg.set_key(key);
        opArg.mutable_minlst()->set_seconds(cdata.minLST.Seconds);
        opArg.mutable_minlst()->set_nanoseconds(cdata.minLST.NanoSeconds);
        std::string serializedArg = opArg.SerializeAsString();

        // call server
        _asyncRpcClient->Call(RPCMethod::InternalGet, serializedArg, state);
        state.FinishEvent.WaitAndReset();
        // parse result
        opResult.ParseFromString(state.Results);
        if (opResult.succeeded()) {

            value = opResult.getvalue();
            cdata.DT.Seconds = opResult.dt().seconds();
            cdata.DT.NanoSeconds = opResult.dt().nanoseconds();

            cdata.DUT.Seconds = opResult.dut().seconds();
            cdata.DUT.NanoSeconds = opResult.dut().nanoseconds();
        }
        return opResult.succeeded();

    }

    bool PartitionKVClient::GetPess(ConsistencyMetadata &cdata, const std::string &key, std::string &value) {

        PbRpcKVInternalGetArg opArg;
        PbRpcKVInternalGetResult opResult;
        AsyncState state;

        // prepare arguments
        opArg.set_key(key);

        opArg.mutable_gst()->set_seconds(cdata.GST.Seconds);
        opArg.mutable_gst()->set_nanoseconds(cdata.GST.NanoSeconds);

        std::string serializedArg = opArg.SerializeAsString();

        // call server
        _asyncRpcClient->Call(RPCMethod::InternalGet, serializedArg, state);
        state.FinishEvent.WaitAndReset();
        // parse result
        opResult.ParseFromString(state.Results);
        if (opResult.succeeded()) {
            value = opResult.getvalue();
            cdata.DT.Seconds = opResult.dt().seconds();
            cdata.DT.NanoSeconds = opResult.dt().nanoseconds();

            cdata.GST.Seconds = opResult.gst().seconds();
            cdata.GST.NanoSeconds = opResult.gst().nanoseconds();
        }

        return opResult.succeeded();
    }

#endif

    bool PartitionKVClient::Get(ConsistencyMetadata &cdata,
                                const std::string &key,
                                std::string &value) {
        if (SysConfig::OptimisticMode) {
            return GetOpt(cdata, key, value);
        }
        return GetPess(cdata, key, value);
    }

    bool PartitionKVClient::Set(ConsistencyMetadata &cdata,
                                const std::string &key,
                                const std::string &value) {
        PbRpcKVInternalSetArg opArg;
        PbRpcKVInternalSetResult opResult;
        AsyncState state;

        opArg.set_key(key);
        opArg.set_value(value);

#ifdef DEP_VECTORS
        for(int i=0;i<cdata.DV.size();i++){
            PbPhysicalTimeSpec *p = opArg.add_dv();
            p->set_seconds(cdata.DV[i].Seconds);
            p->set_nanoseconds(cdata.DV[i].NanoSeconds);
        }

#else
        opArg.mutable_dt()->set_seconds(cdata.DT.Seconds);
        opArg.mutable_dt()->set_nanoseconds(cdata.DT.NanoSeconds);
#endif //DEP_VECTORS

        std::string serializedArg = opArg.SerializeAsString();

        // call server
        _asyncRpcClient->Call(RPCMethod::InternalSet, serializedArg, state);
        state.FinishEvent.WaitAndReset();

        // parse result
        opResult.ParseFromString(state.Results);

        if (opResult.succeeded()) {
            cdata.DT.Seconds = opResult.dt().seconds();
            cdata.DT.NanoSeconds = opResult.dt().nanoseconds();

#ifdef DEP_VECTORS
            cdata.SrcReplica = opResult.srcreplica();
#endif
        }

        return opResult.succeeded();
    }

    bool PartitionKVClient::TxSliceGet(ConsistencyMetadata &cdata,
                                       const std::string &key,
                                       std::string &value) {

        PbRpcKVInternalTxSliceGetArg opArg;
        PbRpcKVInternalTxSliceGetResult opResult;
        AsyncState state;

        // Set the key to be read
        opArg.set_key(key);


#ifndef DEP_VECTORS //SCALAR
        opArg.mutable_st()->set_seconds(cdata.GST.Seconds);
        opArg.mutable_st()->set_nanoseconds(cdata.GST.NanoSeconds);
        std::string serializedArg = opArg.SerializeAsString();

        // call server
        _asyncRpcClient->Call(RPCMethod::InternalTxSliceGet, serializedArg, state);
        state.FinishEvent.WaitAndReset();

        // parse result
        opResult.ParseFromString(state.Results);
        if (opResult.succeeded()) {
            value = opResult.getvalue();
            cdata.DT.Seconds = opResult.dt().seconds();
            cdata.DT.NanoSeconds = opResult.dt().nanoseconds();
#ifdef MEASURE_STATISTICS
            cdata.waited_xact = opResult.waitedxact(); //Setting just to normalize overhead
#endif
        }
#else //VECTOR
        for (int i = 0; i < cdata.GSV.size(); i++) {
            PbPhysicalTimeSpec *vv = opArg.add_sv();
            vv->set_seconds(cdata.GSV[i].Seconds);
            vv->set_nanoseconds(cdata.GSV[i].NanoSeconds);
        }
            opArg.mutable_st()->set_seconds(cdata.DT.Seconds);
            opArg.mutable_st()->set_nanoseconds(cdata.DT.NanoSeconds);

        std::string serializedArg = opArg.SerializeAsString();
    // call server
        _asyncRpcClient->Call(RPCMethod::InternalTxSliceGet, serializedArg, state);
        state.FinishEvent.WaitAndReset();
        opResult.ParseFromString(state.Results);
        if (opResult.succeeded()) {
            value = opResult.getvalue();
            //We do not return the GSV back in xacts
            cdata.DT.Seconds = opResult.dt().seconds();
            cdata.DT.NanoSeconds = opResult.dt().nanoseconds();
            cdata.SrcReplica = opResult.srcreplica();
#ifdef MEASURE_STATISTICS
             cdata.waited_xact = opResult.waitedxact();
#endif
        }

#endif //DEP_VECTORS

        return opResult.succeeded();
    }


#ifdef PARALLEL_XACTS
    void PartitionKVClient::SendParallelInternalTxSliceGetResult(PbRpcKVInternalTxSliceGetResult &opResult){
    std::string serializedArg = opResult.SerializeAsString();
    _asyncRpcClient->CallWithNoState(RPCMethod::ParallelInternalTxSliceGetResult, serializedArg);
    }

    void PartitionKVClient::ParallelTxSliceGet(ConsistencyMetadata &cdata,
                                       const std::string &key,
                                       std::string &value, unsigned long id, int src) {
        PbRpcKVInternalTxSliceGetArg opArg;
        PbRpcKVInternalTxSliceGetResult opResult;

        // Set the key to be read and the src node
        opArg.set_key(key);
        opArg.set_src(src);
        opArg.set_id(id);

#ifndef DEP_VECTORS //SCALAR
        opArg.mutable_st()->set_seconds(cdata.GST.Seconds);
        opArg.mutable_st()->set_nanoseconds(cdata.GST.NanoSeconds);

#else //VECTOR
        for (int i = 0; i < cdata.GSV.size(); i++) {
            PbPhysicalTimeSpec *vv = opArg.add_sv();
            vv->set_seconds(cdata.GSV[i].Seconds);
            vv->set_nanoseconds(cdata.GSV[i].NanoSeconds);
        }
            opArg.mutable_st()->set_seconds(cdata.DT.Seconds);
            opArg.mutable_st()->set_nanoseconds(cdata.DT.NanoSeconds);

#endif //DEP_VECTORS
            std::string serializedArg = opArg.SerializeAsString();
            _asyncRpcClient->CallWithNoState(RPCMethod::InternalTxSliceGet, serializedArg);
    }

#endif //parallel_xacts


    bool PartitionKVClient::ShowItem(const std::string &key, std::string &itemVersions) {
        PbRpcKVInternalShowItemArg args;
        std::string serializedArgs;
        PbRpcKVInternalShowItemResult result;
        AsyncState state;

        // prepare arguments
        args.set_key(key);
        serializedArgs = args.SerializeAsString();

        // call server
        _asyncRpcClient->Call(RPCMethod::InternalShowItem, serializedArgs, state);
        state.FinishEvent.WaitAndReset();

        // parse result
        result.ParseFromString(state.Results);
        if (result.succeeded()) {
            itemVersions = result.itemversions();
        }

        return result.succeeded();
    }

    void PartitionKVClient::InitializePartitioning(DBPartition source) {
        PbPartition opArg;
        opArg.set_name(source.Name);
        opArg.set_publicport(source.PublicPort);
        opArg.set_partitionport(source.PartitionPort);
        opArg.set_replicationport(source.ReplicationPort);
        opArg.set_partitionid(source.PartitionId);
        opArg.set_replicaid(source.ReplicaId);

        std::string serializedArg = opArg.SerializeAsString();

        // call server
        _syncRpcClient->Call(RPCMethod::InitializePartitioning, serializedArg);
    }

    void PartitionKVClient::SendLST(PhysicalTimeSpec lst, int round) {
        PbRpcLST pb_lst;
        pb_lst.mutable_time()->set_seconds(lst.Seconds);
        pb_lst.mutable_time()->set_nanoseconds(lst.NanoSeconds);
        pb_lst.set_round(round);

        std::string serializedArg = pb_lst.SerializeAsString();

#ifdef MEASURE_STATISTICS
        SysStats::NumSendInternalLSTBytes += serializedArg.size();
#endif
        // call server
        _syncRpcClient->Call(RPCMethod::SendLST, serializedArg);
    }

    void PartitionKVClient::SendGST(PhysicalTimeSpec gst) {
        PbRpcGST pb_gst;
        pb_gst.mutable_time()->set_seconds(gst.Seconds);
        pb_gst.mutable_time()->set_nanoseconds(gst.NanoSeconds);

        std::string serializedArg = pb_gst.SerializeAsString();

#ifdef MEASURE_STATISTICS
        SysStats::NumSentGSTBytes += serializedArg.size();
#endif
        // call server
        _syncRpcClient->Call(RPCMethod::SendGST, serializedArg);
    }

#ifdef DEP_VECTORS
    void PartitionKVClient::SendPVV(std::vector<PhysicalTimeSpec> pvv, int round) {
        PbRpcPVV pb_lst;
        for (int i = 0; i < pvv.size(); i++) {
            PbPhysicalTimeSpec *p = pb_lst.add_pvv();
            p->set_seconds(pvv[i].Seconds);
            p->set_nanoseconds(pvv[i].NanoSeconds);
        }
        pb_lst.set_round(round);

        std::string serializedArg = pb_lst.SerializeAsString();

#ifdef MEASURE_STATISTICS
        SysStats::NumSendInternalLSTBytes += serializedArg.size();
#endif
        // call server
        _syncRpcClient->Call(RPCMethod::SendPVV, serializedArg);
    }

    void PartitionKVClient::SendGSV(std::vector<PhysicalTimeSpec> gsv) {
        PbRpcGSV pb_gsv;
        for (int i = 0; i < gsv.size(); i++) {
            PbPhysicalTimeSpec *p = pb_gsv.add_gsv();
            p->set_seconds(gsv[i].Seconds);
            p->set_nanoseconds(gsv[i].NanoSeconds);
        }

        std::string serializedArg = pb_gsv.SerializeAsString();

#ifdef MEASURE_STATISTICS

        SysStats::NumSentGSTBytes += serializedArg.size();
#endif

        // call server
        _syncRpcClient->Call(RPCMethod::SendGSV, serializedArg);
    }

#endif
}
