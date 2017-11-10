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


#include "kvservice/replication_kv_client.h"
#include "common/sys_config.h"
#include "common/sys_stats.h"


namespace scc {

  ReplicationKVClient::ReplicationKVClient(std::string serverName, int replicationPort, int replicaId)
  : _serverName(serverName), _replicaId(replicaId),
  _serverPort(replicationPort) {
    _rpcClient = new SyncRPCClient(_serverName, _serverPort);
  }

  ReplicationKVClient::~ReplicationKVClient() {
    delete _rpcClient;
  }

  void ReplicationKVClient::InitializeReplication(DBPartition source) {
    PbPartition opArg;
    opArg.set_name(source.Name);
    opArg.set_publicport(source.PublicPort);
    opArg.set_partitionport(source.PartitionPort);
    opArg.set_replicationport(source.ReplicationPort);
    opArg.set_partitionid(source.PartitionId);
    opArg.set_replicaid(source.ReplicaId);

    std::string serializedArg = opArg.SerializeAsString();

    // call server
    _rpcClient->Call(RPCMethod::InitializeReplication, serializedArg);
  }

  bool ReplicationKVClient::SendUpdate(std::vector<LocalUpdate*>& updates)
    {
    PbRpcReplicationArg opArg;
    for (unsigned int i = 0; i < updates.size(); i++) {
      opArg.add_updaterecord(updates[i]->SerializedRecord);
    }

    std::string serializedArg = opArg.SerializeAsString();
#ifdef MEASURE_STATISTICS
        SysStats::NumSentReplicationBytes+=serializedArg.size();
#endif

    // call server
    _rpcClient->Call(RPCMethod::ReplicateUpdate, serializedArg);
        return true;
  }

  bool ReplicationKVClient::SendHeartbeat(Heartbeat& hb) {
    PbRpcHeartbeat pb_hb;

    pb_hb.mutable_physicaltime()->set_seconds(hb.PhysicalTime.Seconds);
    pb_hb.mutable_physicaltime()->set_nanoseconds(hb.PhysicalTime.NanoSeconds);
    pb_hb.set_logicaltime(hb.LogicalTime);

    std::string serializedArg = pb_hb.SerializeAsString();

    // call server
    _rpcClient->Call(RPCMethod::SendHeartbeat, serializedArg);

    return true;
  }

}
