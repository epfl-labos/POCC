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


#include "common/sys_logger.h"
#include "common/sys_config.h"
#include "common/utils.h"
#include "common/types.h"
#include "common/exceptions.h"
#include "kvservice/kv_server.h"
#include <google/protobuf/text_format.h>
#include "rpc/rpc_id.h"
#include <thread>
#include <chrono>
#include <assert.h>
#include <unistd.h>
#include <vector>
#include <iostream>

//#define  DEP_VECTORS
//#define PARALLEL_XACTS
namespace scc {

    KVServer::KVServer(std::string name, unsigned short publicPort, int totalNumKeys) {
        //_serverName = Utils::GetHostName();
        _serverName = name;
        _publicPort = publicPort;
        _coordinator = new Coordinator(_serverName, publicPort, totalNumKeys);
    }

    KVServer::KVServer(std::string name,
                       unsigned short publicPort,
                       unsigned short partitionPort,
                       unsigned short replicationPort,
                       int partitionId,
                       int replicaId,
                       int totalNumKeys,
                       std::string groupServerName,
                       int groupServerPort) {
        _serverName = name;
        _publicPort = publicPort;
        _partitionPort = partitionPort;
        _replicationPort = replicationPort;
        _partitionId = partitionId;
        _replicaId = replicaId;
        _coordinator = new Coordinator(_serverName,
                                       _publicPort,
                                       _partitionPort,
                                       _replicationPort,
                                       partitionId,
                                       replicaId,
                                       totalNumKeys,
                                       groupServerName,
                                       groupServerPort);
    }

    KVServer::~KVServer() {
        delete _coordinator;
    }

    void KVServer::Profile() {
        /*ProfilerStart("/output/prof.prof");
        std::this_thread::sleep_for(std::chrono::milliseconds(60000));
        ProfilerStop();
        exit(0);
         */
    }

    void KVServer::Run() {
        // partitions of the same replication group
        std::thread tPartition(&KVServer::ServePartitionConnection, this);
        tPartition.detach();

        //std::thread tprof(&KVServer::Profile, this);
        //tprof.detach();

        // replicas of the same partition
        std::thread tReplication(&KVServer::ServeReplicationConnection, this);
        tReplication.detach();

        // initialize
        _coordinator->Initialize();

        // serve operation request from clients
        ServePublicConnection();
    }

    void KVServer::ServePublicConnection() {
        try {
            TCPServerSocket serverSocket(_publicPort);
            for (;;) {
                TCPSocket *clientSocket = serverSocket.accept();
                std::thread t(&KVServer::HandlePublicRequest, this, clientSocket);
                t.detach();
            }
        } catch (SocketException &e) {
            fprintf(stdout, "Server public socket exception: %s\n", e.what());
            fflush(stdout);
        }
        catch (...) {
            fprintf(stdout, "[SEVERE FAILURE ServePublicConnection] Server public socket exception: %s\n", "...");
            fflush(stdout);
        }
    }

    void KVServer::HandlePublicRequest(TCPSocket *clientSocket) {
        try {
            RPCServerPtr rpcServer(new RPCServer(clientSocket));
            for (;;) {

                // get rpc request
                PbRpcRequest rpcRequest;
                rpcServer->RecvRequest(rpcRequest);
                bool stop = false;

                while (!stop) {
                    try {
                        processPublicRequest(rpcServer, rpcRequest);
                        break;
                    }
                    catch (...) {
                        fprintf(stdout, "[PUBLIC OPERATION FAILURE] HandlePublicRequest\n");
                        fflush(stdout);
                        try {
                            std::this_thread::sleep_for(std::chrono::milliseconds(100));
                            rpcServer->RecvRequest(rpcRequest);
                            stop = true;
                        }
                        catch (...) {
                            fprintf(stdout, "[FAILED RETRY] HandlePublicRequest ...\n");
                            fflush(stdout);
                        }
                    }
                }
            }
        } catch (SocketException &e) {

            fprintf(stdout, "HandlePublicRequest:Client serving thread socket exception: %s\n", e.what());
            fflush(stdout);
        }
        catch (...) {
            fprintf(stdout, "[SOCKET FAILURE HandlePublicRequest] Client serving thread socket exception(P): %s\n",
                    "...");
            fflush(stdout);
        }
    }


    void KVServer::processPublicRequest(RPCServerPtr &rpcServer, PbRpcRequest &rpcRequest) {
        Shared lock(_coordinator->mtx);

        PbRpcReply rpcReply;

        switch (static_cast<RPCMethod> (rpcRequest.methodid())) {
            ////////////////////////////////
            // key-value store interface  //
            ////////////////////////////////

            case RPCMethod::GetServerConfig: {
                PbRpcKVPublicGetServerConfigResult opResult;
                opResult.set_succeeded(true);
                opResult.set_numpartitions(_coordinator->NumPartitions());
                opResult.set_numreplicasperpartition(_coordinator->NumReplicasPerPartition());
                opResult.set_optimisticmode(SysConfig::OptimisticMode);
                opResult.set_replicaid(_coordinator->ReplicaId());

                // send rpc reply
                rpcReply.set_msgid(rpcRequest.msgid());
                rpcReply.set_results(opResult.SerializeAsString());
                rpcServer->SendReply(rpcReply);
                break;
            }

            case RPCMethod::EchoTest: {
                // get arguments
                PbRpcEchoTest echoRequest;
                PbRpcEchoTest echoReply;
                echoRequest.ParseFromString(rpcRequest.arguments());
                echoReply.set_text(echoRequest.text());
                // send rpc reply
                rpcReply.set_msgid(rpcRequest.msgid());
                rpcReply.set_results(echoReply.SerializeAsString());
                rpcServer->SendReply(rpcReply);
                break;
            }


            case RPCMethod::Get: {
                // get arguments
                PbRpcKVPublicGetArg opArg;
                PbRpcKVPublicGetResult opResult;
                opArg.ParseFromString(rpcRequest.arguments());

                if (SysConfig::OptimisticMode) {
                    HandleGetOpt(opArg, opResult);
                } else {
                    HandleGetPess(opArg, opResult);
                }
                // send rpc reply
                rpcReply.set_msgid(rpcRequest.msgid());
                rpcReply.set_results(opResult.SerializeAsString());
                rpcServer->SendReply(rpcReply);
                break;
            }


            case RPCMethod::Set: {
                // get arguments
                PbRpcKVPublicSetArg opArg;
                PbRpcKVPublicSetResult opResult;
                opArg.ParseFromString(rpcRequest.arguments());

                ConsistencyMetadata cdata;

#ifdef DEP_VECTORS
                cdata.MaxElId = 0;

                for (int i = 0; i < _coordinator->NumReplicasPerPartition(); i++) {
                    const PbPhysicalTimeSpec &v = opArg.dv(i);
                    PhysicalTimeSpec p(v.seconds(), v.nanoseconds());
                    cdata.DV.push_back(p);
                    if (cdata.DV[cdata.MaxElId] < p) {
                        cdata.MaxElId = i;
                    }
                }

#else
                cdata.DT.Seconds = opArg.dt().seconds();
                cdata.DT.NanoSeconds = opArg.dt().nanoseconds();
#endif //DEP_VECTORS

                // execute operation
                bool ret = _coordinator->Set(cdata, opArg.key(), opArg.value());

                opResult.set_succeeded(ret);
                if (ret) {
                    opResult.mutable_dt()->set_seconds(cdata.DT.Seconds);
                    opResult.mutable_dt()->set_nanoseconds(cdata.DT.NanoSeconds);
                }
                // send rpc reply
                rpcReply.set_msgid(rpcRequest.msgid());
                rpcReply.set_results(opResult.SerializeAsString());
                rpcServer->SendReply(rpcReply);
                break;
            }

            case RPCMethod::TxGet: {
                // get arguments
                PbRpcKVPublicTxGetArg opArg;
                PbRpcKVPublicTxGetResult opResult;
                opArg.ParseFromString(rpcRequest.arguments());

                HandleTxGet(opArg, opResult);

                // send rpc reply
                rpcReply.set_msgid(rpcRequest.msgid());
                rpcReply.set_results(opResult.SerializeAsString());
                rpcServer->SendReply(rpcReply);
                break;
            }

            case RPCMethod::ShowItem: {
                // get arguments
                PbRpcKVPublicShowArg opArg;
                PbRpcKVPublicShowResult opResult;
                opArg.ParseFromString(rpcRequest.arguments());
                // execute op
                std::string itemVersions;
                bool ret = _coordinator->ShowItem(opArg.key(), itemVersions);
                opResult.set_succeeded(ret);
                if (ret) {
                    opResult.set_returnstring(itemVersions);
                }
                // send rpc reply
                rpcReply.set_msgid(rpcRequest.msgid());
                rpcReply.set_results(opResult.SerializeAsString());
                rpcServer->SendReply(rpcReply);
                break;
            }

            case RPCMethod::ShowDB: {
                // get arguments
                PbRpcKVPublicShowResult opResult;
                // execute op
                std::string allItemVersions;
                bool ret = _coordinator->ShowDB(allItemVersions);
                opResult.set_succeeded(ret);
                if (ret) {
                    opResult.set_returnstring(allItemVersions);
                }
                // send rpc reply
                rpcReply.set_msgid(rpcRequest.msgid());
                rpcReply.set_results(opResult.SerializeAsString());
                rpcServer->SendReply(rpcReply);
                break;
            }
            case RPCMethod::ShowState: {
                // get arguments
                PbRpcKVPublicShowResult opResult;
                // execute op
                std::string stateStr = (boost::format("*%s|") % _serverName).str();
                bool ret = _coordinator->ShowState(stateStr);
                opResult.set_succeeded(ret);
                if (ret) {
                    opResult.set_returnstring(stateStr);
                }
                // send rpc reply
                rpcReply.set_msgid(rpcRequest.msgid());
                rpcReply.set_results(opResult.SerializeAsString());
                rpcServer->SendReply(rpcReply);

                break;
            }

            case RPCMethod::DumpLatencyMeasurement: {
                // get arguments
                PbRpcKVPublicShowResult opResult;
                // execute op
                std::string resultStr;
                bool ret = _coordinator->DumpLatencyMeasurement(resultStr);
                opResult.set_succeeded(ret);
                if (ret) {
                    opResult.set_returnstring(resultStr);
                }
                // send rpc reply
                rpcReply.set_msgid(rpcRequest.msgid());
                rpcReply.set_results(opResult.SerializeAsString());
                rpcServer->SendReply(rpcReply);
                break;
            }
            default:
                throw KVOperationException("(public) Unsupported operation.");
        }
    }


    void KVServer::ServePartitionConnection() {
        try {
            TCPServerSocket serverSocket(_partitionPort);
            for (;;) {
                TCPSocket *clientSocket = serverSocket.accept();
                std::thread t(&KVServer::HandlePartitionRequest, this, clientSocket);
                t.detach();
            }
        } catch (SocketException &e) {
            fprintf(stdout, "Server partition socket exception: %s\n", e.what());
            fflush(stdout);
        }
        catch (...) {
            fprintf(stdout, "SEVERE Server partition socket exception: %s\n", "...");
            fflush(stdout);
        }
    }


    void KVServer::HandlePartitionRequest(TCPSocket *clientSocket) {
        try {
            RPCServerPtr rpcServer(new RPCServer(clientSocket));
            DBPartition servedPartition;

            for (;;) {
                // get rpc request
                PbRpcRequest rpcRequest;
                rpcServer->RecvRequest(rpcRequest);

                bool done = false;

                while (!done) {
                    try {
                        processPartitionRequest(rpcServer, rpcRequest, servedPartition);
                        done = true;
                    }
                    catch (...) {
                        fprintf(stdout, "[PARTITION OPERATION FAILURE] HandlePartitionRequest :( \n");
                        fflush(stdout);

                        try {
                            std::this_thread::sleep_for(std::chrono::milliseconds(100));
                            rpcServer->RecvRequest(rpcRequest);
                        }
                        catch (...) {
                            fprintf(stdout, "[FAILED RETRY] HandlePublicRequest ...\n");
                            fflush(stdout);
                        }
                    }
                }
            }
        } catch (SocketException &e) {

            fprintf(stdout, "Partition serving thread socket exception: %s\n", e.what());
            fflush(stdout);
        }
        catch (...) {
            fprintf(stdout, "[SOCKET FAILURE] HandlePartitionRequest]\n");
            fflush(stdout);
        }
    }

    void
    KVServer::processPartitionRequest(RPCServerPtr &rpcServer, PbRpcRequest &rpcRequest, DBPartition &servedPartition) {
        PbRpcReply rpcReply;
        Shared lock(_coordinator->mtx);

        switch (static_cast<RPCMethod> (rpcRequest.methodid())) {
            case RPCMethod::InternalGet: {
                // get arguments
                PbRpcKVInternalGetArg opArg;
                PbRpcKVInternalGetResult opResult;
                opArg.ParseFromString(rpcRequest.arguments());

                if (SysConfig::OptimisticMode) {
                    HandleInternalGetOpt(opArg, opResult);
                } else {
                    HandleInternalGetPess(opArg, opResult);
                }

                // send rpc reply
                rpcReply.set_msgid(rpcRequest.msgid());
                rpcReply.set_results(opResult.SerializeAsString());
                rpcServer->SendReply(rpcReply);
                break;
            }

            case RPCMethod::InternalSet: {
                // get arguments
                PbRpcKVInternalSetArg opArg;
                PbRpcKVInternalSetResult opResult;
                opArg.ParseFromString(rpcRequest.arguments());

                ConsistencyMetadata cdata;

#ifndef DEP_VECTORS
                cdata.DT.Seconds = opArg.dt().seconds();
                cdata.DT.NanoSeconds = opArg.dt().nanoseconds();


#else //DEP_VECTORS
                cdata.MaxElId = 0;

                for (int i = 0; i < _coordinator->NumReplicasPerPartition(); i++) {
                    const PbPhysicalTimeSpec &v = opArg.dv(i);
                    PhysicalTimeSpec p(v.seconds(), v.nanoseconds());
                    cdata.DV.push_back(p);
                    if (cdata.DV[cdata.MaxElId] < p) {
                        cdata.MaxElId = i;
                    }
                }
#endif //DEP_VECTORS
                // execute op
                bool ret = _coordinator->InternalSet(cdata, opArg.key(), opArg.value());

                opResult.set_succeeded(ret);
                if (ret) {
                    opResult.mutable_dt()->set_seconds(cdata.DT.Seconds);
                    opResult.mutable_dt()->set_nanoseconds(cdata.DT.NanoSeconds);
                }

                // send rpc reply
                rpcReply.set_msgid(rpcRequest.msgid());
                rpcReply.set_results(opResult.SerializeAsString());
                rpcServer->SendReply(rpcReply);
                break;
            }

//#ifndef PARALLEL_XACTS
            case RPCMethod::InternalTxSliceGet: {
                // get arguments
                PbRpcKVInternalTxSliceGetArg opArg;
                PbRpcKVInternalTxSliceGetResult opResult;
                opArg.ParseFromString(rpcRequest.arguments());
                HandleInternalTxSliceGet(opArg, opResult);
#ifndef PARALLEL_XACTS
                // send rpc reply
                rpcReply.set_msgid(rpcRequest.msgid());
                rpcReply.set_results(opResult.SerializeAsString());
                rpcServer->SendReply(rpcReply);
#else
                opResult.set_id(opArg.id());
                opResult.set_src(_partitionId);
                int src = opArg.src();
                _coordinator->C_SendParallelInternalTxSliceGetResult(opResult, src);
#endif
                break;
            }
//#endif //parallel_xacts

#ifdef PARALLEL_XACTS
            case RPCMethod::ParallelInternalTxSliceGetResult: {
                PbRpcKVInternalTxSliceGetResult opResult;
                opResult.ParseFromString(rpcRequest.arguments());
                ParallelXact *p_x = _coordinator->GetParallelXact(opResult.id());
                int part = opResult.src();
                int index = GET_PART_TO_KEY(p_x, part);//p_x->GetPartToKey(part);

                STORE_RESULT(p_x, index, opResult);
                p_x->_waitHandle.SetIfCountZero();

                break;
            }
#endif //PARALLEL_XACT
            case RPCMethod::InternalShowItem: {
                // get arguments
                PbRpcKVInternalShowItemArg opArg;
                PbRpcKVInternalShowItemResult opResult;
                opArg.ParseFromString(rpcRequest.arguments());
                // execute op
                std::string itemVersions;
                bool ret = _coordinator->ShowItem(opArg.key(), itemVersions);
                opResult.set_succeeded(ret);
                if (ret) {
                    opResult.set_itemversions(itemVersions);
                }
                // send rpc reply
                rpcReply.set_msgid(rpcRequest.msgid());
                rpcReply.set_results(opResult.SerializeAsString());
                rpcServer->SendReply(rpcReply);
                break;
            }

            case RPCMethod::InitializePartitioning: {
                PbPartition opArg;
                opArg.ParseFromString(rpcRequest.arguments());

                servedPartition.Name = opArg.name();
                servedPartition.PublicPort = opArg.publicport();
                servedPartition.PartitionPort = opArg.partitionport();
                servedPartition.ReplicationPort = opArg.replicationport();
                servedPartition.PartitionId = opArg.partitionid();
                servedPartition.ReplicaId = opArg.replicaid();
                break;
            }

            case RPCMethod::SendLST: {
                PbRpcLST pb_lst;
                std::string arg;
                pb_lst.ParseFromString(arg = rpcRequest.arguments());
#ifdef MEASURE_STATISTICS
                SysStats::NumRecvInternalLSTBytes += arg.size();
#endif
                PhysicalTimeSpec lst;
                lst.Seconds = pb_lst.time().seconds();
                lst.NanoSeconds = pb_lst.time().nanoseconds();
                int round = pb_lst.round();

                if (SysConfig::GSTDerivationMode == GSTDerivationType::TREE) {
                    _coordinator->HandleLSTFromChildren(lst, round);

                } else {
                    std::cout << "Non-existing GSTDerivationType: \n";
                }
                break;
            }

            case RPCMethod::SendGST: {
                PbRpcGST pb_gst;
                std::string arg;
                pb_gst.ParseFromString(arg = rpcRequest.arguments());

#ifdef MEASURE_STATISTICS
                SysStats::NumRecvGSTBytes += arg.size();
#endif
                PhysicalTimeSpec gst;
                gst.Seconds = pb_gst.time().seconds();
                gst.NanoSeconds = pb_gst.time().nanoseconds();
                _coordinator->HandleGSTFromParent(gst);

                break;
            }

#ifdef DEP_VECTORS
            case RPCMethod::SendPVV: {
                PbRpcPVV pb_lst;
                std::string arg;
                pb_lst.ParseFromString(arg = rpcRequest.arguments());
#ifdef MEASURE_STATISTICS
                SysStats::NumRecvInternalLSTBytes += arg.size();
#endif
                std::vector<PhysicalTimeSpec> inputPVV;
                for (int i = 0; i < pb_lst.pvv().size(); i++) {
                    const PbPhysicalTimeSpec &v = pb_lst.pvv(i);
                    PhysicalTimeSpec p(v.seconds(), v.nanoseconds());
                    inputPVV.push_back(p);
                }
                int round = pb_lst.round();
                if (SysConfig::GSTDerivationMode == GSTDerivationType::TREE) {
                    _coordinator->HandlePVVFromChildren(inputPVV, round);
                } else {
                    assert(false);
                }
                break;
            }
            case RPCMethod::SendGSV: {
                PbRpcGSV pb_gst;
                std::string arg;
                pb_gst.ParseFromString(arg = rpcRequest.arguments());
#ifdef MEASURE_STATISTICS
                SysStats::NumRecvGSTBytes += arg.size();
#endif
                std::vector<PhysicalTimeSpec> inputGSV;
                for (int i = 0; i < pb_gst.gsv().size(); i++) {
                    const PbPhysicalTimeSpec &v = pb_gst.gsv(i);
                    PhysicalTimeSpec p(v.seconds(), v.nanoseconds());
                    inputGSV.push_back(p);
                }
                _coordinator->HandleGSVFromParent(inputGSV);
                break;
            }

#endif //DEP_VECTORS
            default:
                throw KVOperationException("Non-supported (partition) operation.");
        }
    }


    void KVServer::ServeReplicationConnection() {
        try {
            TCPServerSocket serverSocket(_replicationPort);
            for (;;) {
                TCPSocket *clientSocket = serverSocket.accept();
                std::thread t(&KVServer::HandleReplicationRequest, this, clientSocket);
                t.detach();
            }
        } catch (SocketException &e) {

            fprintf(stdout, "Server replication socket exception: %s\n", e.what());
            fflush(stdout);
        }
        catch (...) {
            fprintf(stdout, "[SEVERE FAILURE ServeReplicationConnection] Server replication socket exception: %s\n",
                    "...");
            fflush(stdout);
        }
    }

    void KVServer::HandleReplicationRequest(TCPSocket *clientSocket) {
        try {
            RPCServerPtr rpcServer(new RPCServer(clientSocket));
            DBPartition servedPartition;

            for (;;) {
                // get rpc request
                PbRpcRequest rpcRequest;
                rpcServer->RecvRequest(rpcRequest);

                bool done = false;

                while (!done) {
                    try {
                        processReplicationRequest(rpcServer, rpcRequest, servedPartition);
                        done = true;
                    }
                    catch (...) {
                        fprintf(stdout, "[OPERATION FAILURE] HandleReplicationRequest :( \n");
                        fflush(stdout);

                        try {
                            std::this_thread::sleep_for(std::chrono::milliseconds(100));
                            rpcServer->RecvRequest(rpcRequest);
                        }
                        catch (...) {
                            fprintf(stdout, "[FAILED RETRY] HandleReplicationRequest ...\n");
                            fflush(stdout);
                        }
                    }
                }
            }
        } catch (SocketException &e) {

            fprintf(stdout, "Replication serving thread socket exception: %s\n", e.what());
            fflush(stdout);
        }
        catch (...) {
            fprintf(stdout, "[SOCKET FAILURE] HandlePartitionRequest]\n");
            fflush(stdout);
        }
    }


    //NB: this is a single thread per replica

    void KVServer::processReplicationRequest(RPCServerPtr &rpcServer, PbRpcRequest &rpcRequest,
                                             DBPartition &servedPartition) {
        PbRpcReply rpcReply;
        Shared lock(_coordinator->mtx);

        switch (static_cast<RPCMethod> (rpcRequest.methodid())) {
            case RPCMethod::InitializeReplication: {
                PbPartition opArg;
                opArg.ParseFromString(rpcRequest.arguments());

                servedPartition.Name = opArg.name();
                servedPartition.PublicPort = opArg.publicport();
                servedPartition.PartitionPort = opArg.partitionport();
                servedPartition.ReplicationPort = opArg.replicationport();
                servedPartition.PartitionId = opArg.partitionid();
                servedPartition.ReplicaId = opArg.replicaid();

                break;
            }
            case RPCMethod::ReplicateUpdate: {
                // get arguments
                PbRpcReplicationArg opArg;
                std::string arg;
                opArg.ParseFromString(arg = rpcRequest.arguments());
#ifdef MEASURE_STATISTICS
                SysStats::NumRecvReplicationBytes += arg.size();
#endif
                // apply propagated updates
                std::vector < PropagatedUpdate * > updates;

                for (int i = 0; i < opArg.updaterecord_size(); i++) {
                    PbLogSetRecord record;
                    const std::string &serializedRecord = opArg.updaterecord(i);
                    record.ParseFromString(serializedRecord);

                    PropagatedUpdate *update = new PropagatedUpdate();

                    update->SerializedRecord = serializedRecord;

                    update->SrcReplica = record.srcreplica();
                    update->Key = record.key();
                    update->Value = record.value();

                    update->PUT.Seconds = record.put().seconds();
                    update->PUT.NanoSeconds = record.put().nanoseconds();

#ifndef DEP_VECTORS  //SCALAR
                    if (SysConfig::OptimisticMode) {
                        update->PDUT.Seconds = record.pdut().seconds();
                        update->PDUT.NanoSeconds = record.pdut().nanoseconds();
                    }
#else //VECTOR
                    update->DV.resize(_coordinator->NumReplicasPerPartition());

                    for (int j = 0; j < _coordinator->NumReplicasPerPartition(); j++) {
                        const PbPhysicalTimeSpec &v = record.dv(j);
                        PhysicalTimeSpec p(v.seconds(), v.nanoseconds());
                        update->DV[j] = p;
                    }
#endif //DEP_VECTORS
                    updates.push_back(update);
                }
                _coordinator->HandlePropagatedUpdate(updates);

                break;
            }
            case RPCMethod::SendHeartbeat: {
                // get arguments
                PbRpcHeartbeat pb_hb;

                pb_hb.ParseFromString(rpcRequest.arguments());

                Heartbeat hb;
                hb.PhysicalTime.Seconds = pb_hb.physicaltime().seconds();
                hb.PhysicalTime.NanoSeconds = pb_hb.physicaltime().nanoseconds();
                hb.LogicalTime = pb_hb.logicaltime();

                _coordinator->HandleHeartbeat(hb, servedPartition.ReplicaId);

                break;
            }
            default:
                throw KVOperationException("(replication) Unsupported operation\n");
        }
    }


#ifdef DEP_VECTORS
    // //////////////////////////// VECTOR ///////////////////////////////////////

    void KVServer::HandleInternalGetOpt(PbRpcKVInternalGetArg &opArg, PbRpcKVInternalGetResult &opResult) {
        ConsistencyMetadata cdata;
        for (int i = 0; i < _coordinator->NumReplicasPerPartition(); i++) {
            const PbPhysicalTimeSpec &v = opArg.ndv(i);
            PhysicalTimeSpec p(v.seconds(), v.nanoseconds());
            cdata.NDV.push_back(p);
        }
        std::string getValue;
        // get arguments
        bool ret = _coordinator->InternalGet(cdata, opArg.key(), getValue);

        opResult.set_succeeded(ret);
        if (ret) {
            opResult.set_getvalue(getValue);

#ifndef EMBEDDED_DT_IN_SCRREP_DV
            opResult.mutable_dt()->set_seconds(cdata.DT.Seconds);
            opResult.mutable_dt()->set_nanoseconds(cdata.DT.NanoSeconds);
#endif

            for (int i = 0; i < _coordinator->NumReplicasPerPartition(); i++) {
                PbPhysicalTimeSpec *p = opResult.add_dv();
                p->set_seconds(cdata.DV[i].Seconds);
                p->set_nanoseconds(cdata.DV[i].NanoSeconds);
            }
            opResult.set_srcreplica(cdata.SrcReplica);
        }
    }


    void KVServer::HandleInternalGetPess(PbRpcKVInternalGetArg &opArg, PbRpcKVInternalGetResult &opResult) {
        ConsistencyMetadata cdata;
        for (int i = 0; i < _coordinator->NumReplicasPerPartition(); i++) {
            const PbPhysicalTimeSpec &v = opArg.gsv(i);
            PhysicalTimeSpec p(v.seconds(), v.nanoseconds());
            cdata.GSV.push_back(p);
        }
        // execute op
        std::string getValue;
        // get arguments
        bool ret = _coordinator->InternalGet(cdata, opArg.key(), getValue);
        opResult.set_succeeded(ret);
        if (ret) {
            opResult.set_getvalue(getValue);

#ifndef EMBEDDED_DT_IN_SCRREP_DV
            opResult.mutable_dt()->set_seconds(cdata.DT.Seconds);
            opResult.mutable_dt()->set_nanoseconds(cdata.DT.NanoSeconds);
#endif
            for (int i = 0; i < _coordinator->NumReplicasPerPartition(); i++) {
                PbPhysicalTimeSpec *p = opResult.add_gsv();
                p->set_seconds(cdata.GSV[i].Seconds);
                p->set_nanoseconds(cdata.GSV[i].NanoSeconds);
            }
            opResult.set_srcreplica(cdata.SrcReplica);
        }
    }


    void KVServer::HandleGetOpt(PbRpcKVPublicGetArg &opArg, PbRpcKVPublicGetResult &opResult) {
        ConsistencyMetadata cdata;

        for (int i = 0; i < _coordinator->NumReplicasPerPartition(); i++) {
            const PbPhysicalTimeSpec &v = opArg.ndv(i);
            PhysicalTimeSpec p(v.seconds(), v.nanoseconds());
            cdata.NDV.push_back(p);
        }

// execute operation
        std::string getValue;
        bool ret = _coordinator->Get(cdata, opArg.key(), getValue);

        opResult.set_succeeded(ret);
        if (ret) {
            opResult.set_getvalue(getValue);

#ifndef EMBEDDED_DT_IN_SCRREP_DV
            opResult.mutable_dt()->set_seconds(cdata.DT.Seconds);
            opResult.mutable_dt()->set_nanoseconds(cdata.DT.NanoSeconds);
#endif

            for (int i = 0; i < _coordinator->NumReplicasPerPartition(); i++) {
                PbPhysicalTimeSpec *p = opResult.add_dv();
                p->set_seconds(cdata.DV[i].Seconds);
                p->set_nanoseconds(cdata.DV[i].NanoSeconds);
            }
            opResult.set_srcreplica(cdata.SrcReplica);
        }
    }


    void KVServer::HandleGetPess(PbRpcKVPublicGetArg &opArg, PbRpcKVPublicGetResult &opResult) {
        ConsistencyMetadata cdata;
        for (int i = 0; i < _coordinator->NumReplicasPerPartition(); i++) {
            const PbPhysicalTimeSpec &v = opArg.gsv(i);
            PhysicalTimeSpec p(v.seconds(), v.nanoseconds());
            cdata.GSV.push_back(p);
        }
        std::string getValue;
        bool ret = _coordinator->Get(cdata, opArg.key(), getValue);

        opResult.set_succeeded(ret);
        if (ret) {

            opResult.set_getvalue(getValue);

#ifndef EMBEDDED_DT_IN_SCRREP_DV
            opResult.mutable_dt()->set_seconds(cdata.DT.Seconds);
            opResult.mutable_dt()->set_nanoseconds(cdata.DT.NanoSeconds);
#endif

            for (int i = 0; i < _coordinator->NumReplicasPerPartition(); i++) {
                PbPhysicalTimeSpec *p = opResult.add_gsv();
                p->set_seconds(cdata.GSV[i].Seconds);
                p->set_nanoseconds(cdata.GSV[i].NanoSeconds);
            }
            opResult.set_srcreplica(cdata.SrcReplica);

        }
    }

    void KVServer::HandleTxGet(PbRpcKVPublicTxGetArg &opArg, PbRpcKVPublicTxGetResult &opResult) {
        ConsistencyMetadata cdata;
        std::vector<std::string> keySet;
        std::vector<std::string> getValueSet;
        const int numReplicas = _coordinator->NumReplicasPerPartition();

        for (int i = 0; i < opArg.key_size(); ++i) {
            keySet.push_back(opArg.key(i));
        }

        for (int i = 0; i < numReplicas; i++) {
            const PbPhysicalTimeSpec &v = opArg.sv(i);
            PhysicalTimeSpec p(v.seconds(), v.nanoseconds());

            if (SysConfig::OptimisticMode) {
                cdata.NDV.push_back(p);
            } else {
                cdata.GSV.push_back(p);
            }
        }

        const PbPhysicalTimeSpec &v = opArg.st();
        cdata.DT = PhysicalTimeSpec(v.seconds(), v.nanoseconds());

        // execute operation

#ifndef PARALLEL_XACTS
        bool ret = _coordinator->TxGet(cdata, keySet, getValueSet);
#else
        bool ret = _coordinator->ParallelTxGet(cdata, keySet, getValueSet);
#endif

        opResult.set_succeeded(ret);
        if (ret) {
            for (unsigned int i = 0; i < getValueSet.size(); ++i) {
                opResult.add_getvalue(getValueSet[i]);
            }

            //Return update times of read item
            for (int j = 0; j < numReplicas; j++) {
                PbPhysicalTimeSpec *p1 = opResult.add_uv();
                p1->set_seconds(cdata.DV[j].Seconds);
                p1->set_nanoseconds(cdata.DV[j].NanoSeconds);
            }

            //Return the GSV (or USV)
            for (int j = 0; j < numReplicas; j++) {
                PbPhysicalTimeSpec *p1 = opResult.add_sv();
                //In OPTIMISTIC mode GSV is placehlder for the TRANSACTION VECTOR
                p1->set_seconds(cdata.GSV[j].Seconds);
                p1->set_nanoseconds(cdata.GSV[j].NanoSeconds);
            }
        }
    }

    void KVServer::HandleInternalTxSliceGet(PbRpcKVInternalTxSliceGetArg &opArg,
                                            PbRpcKVInternalTxSliceGetResult &opResult) {

        ConsistencyMetadata cdata;
        std::string getValue;
        bool ret;

//GSV is used both for OPT and PESS as a timestamp for the xact

        for (int i = 0; i < _coordinator->NumReplicasPerPartition(); i++) {
            PhysicalTimeSpec p(opArg.sv(i).seconds(), opArg.sv(i).nanoseconds());
            cdata.GSV.push_back(p);
        }
        cdata.DT = PhysicalTimeSpec(opArg.st().seconds(), opArg.st().nanoseconds());

        ret = _coordinator->InternalTxSliceGet(cdata, opArg.key(), getValue);
        opResult.set_succeeded(ret);
        if (ret) {
            opResult.set_getvalue(getValue);
            opResult.mutable_dt()->set_seconds(cdata.DT.Seconds);
            opResult.mutable_dt()->set_nanoseconds(cdata.DT.NanoSeconds);
            opResult.set_srcreplica(cdata.SrcReplica);
#ifdef MEASURE_STATISTICS
            opResult.set_waitedxact(cdata.waited_xact);
#endif
            //We do not attach the GSV or the NDV. We use the SV instead, which is given as input as DV and stored by the coordinator of the xact
        } else {
            fprintf(stdout, "Slice for key %s of xact %d failed. Gonna send %s\n", opArg.key().c_str(), opArg.id(),
                    opResult.DebugString().c_str());
            fflush(stdout);
        }
    }

#else
    // //////////////////////////// SCALAR ///////////////////////////////////////

    void KVServer::HandleInternalGetPess(PbRpcKVInternalGetArg &opArg, PbRpcKVInternalGetResult &opResult) {
        ConsistencyMetadata cdata;
        cdata.GST.Seconds = opArg.gst().seconds();
        cdata.GST.NanoSeconds = opArg.gst().nanoseconds();

        // execute op
        std::string getValue;
        // get arguments
        bool ret = _coordinator->InternalGet(cdata, opArg.key(), getValue);

        opResult.set_succeeded(ret);
        if (ret) {
            opResult.set_getvalue(getValue);
            opResult.mutable_dt()->set_seconds(cdata.DT.Seconds);
            opResult.mutable_dt()->set_nanoseconds(cdata.DT.NanoSeconds);
            opResult.mutable_gst()->set_seconds(cdata.GST.Seconds);
            opResult.mutable_gst()->set_nanoseconds(cdata.GST.NanoSeconds);
        }
    }

    void KVServer::HandleGetPess(PbRpcKVPublicGetArg &opArg, PbRpcKVPublicGetResult &opResult) {
        ConsistencyMetadata cdata;
        cdata.GST.Seconds = opArg.gst().seconds();
        cdata.GST.NanoSeconds = opArg.gst().nanoseconds();
        std::string getValue;
        bool ret = _coordinator->Get(cdata, opArg.key(), getValue);

        opResult.set_succeeded(ret);
        if (ret) {

            opResult.set_getvalue(getValue);
            opResult.mutable_dt()->set_seconds(cdata.DT.Seconds);
            opResult.mutable_dt()->set_nanoseconds(cdata.DT.NanoSeconds);
            opResult.mutable_gst()->set_seconds(cdata.GST.Seconds);
            opResult.mutable_gst()->set_nanoseconds(cdata.GST.NanoSeconds);
        }
    }

    void KVServer::HandleTxGetPess(PbRpcKVPublicTxGetArg &opArg,
                                   PbRpcKVPublicTxGetResult &opResult) {
        ConsistencyMetadata cdata;
        cdata.DT.Seconds = opArg.ut().seconds();
        cdata.DT.NanoSeconds = opArg.ut().nanoseconds();
        cdata.GST.Seconds = opArg.st().seconds();
        cdata.GST.NanoSeconds = opArg.st().nanoseconds();

        std::vector <std::string> keySet;
        for (int i = 0; i < opArg.key_size(); ++i) {
            keySet.push_back(opArg.key(i));
        }
        // execute op
        std::vector <std::string> getValueSet;
#ifndef PARALLEL_XACTS
        bool ret = _coordinator->TxGet(cdata, keySet, getValueSet);
#else
        bool ret = _coordinator->ParallelTxGet(cdata, keySet, getValueSet);

#endif
        opResult.set_succeeded(ret);
        if (ret) {
            for (unsigned int i = 0; i < getValueSet.size(); ++i) {
                opResult.add_getvalue(getValueSet[i]);
            }
            opResult.mutable_ut()->set_seconds(cdata.DT.Seconds);
            opResult.mutable_ut()->set_nanoseconds(cdata.DT.NanoSeconds);
            opResult.mutable_st()->set_seconds(cdata.GST.Seconds);
            opResult.mutable_st()->set_nanoseconds(cdata.GST.NanoSeconds);
        }
    }

    void KVServer::HandleInternalTxSliceGet(PbRpcKVInternalTxSliceGetArg &opArg,
                                            PbRpcKVInternalTxSliceGetResult &opResult) {
        ConsistencyMetadata cdata;
        std::string getValue;
        bool ret;
//GST is used both for OPT and PESS as a timestamp for the xact
        cdata.GST.Seconds = opArg.st().seconds();
        cdata.GST.NanoSeconds = opArg.st().nanoseconds();
        ret = _coordinator->InternalTxSliceGet(cdata, opArg.key(), getValue);

        opResult.set_succeeded(ret);
        if (ret) {
            opResult.set_getvalue(getValue);
            opResult.mutable_dt()->set_seconds(cdata.DT.Seconds);
            opResult.mutable_dt()->set_nanoseconds(cdata.DT.NanoSeconds);
#ifdef MEASURE_STATISTICS
            opResult.set_waitedxact(cdata.waited_xact);
#endif
            //We do not take back the GST:we use the TS as new GST to reduce communication overhead
        }
    }

#endif

} // namespace scc
