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


#include "groupservice/group_client.h"
#include "messages/rpc_messages.pb.h"

namespace scc {

  GroupClient::GroupClient(std::string host, int port)
  : _rpcClient(host, port) { }

  void GroupClient::Echo(const std::string& text, std::string& echoText) {
    PbRpcEchoTest arg;
    std::string serializedArg;
    PbRpcEchoTest result;
    std::string serializedResult;

    // prepare argument
    arg.set_text(text);
    serializedArg = arg.SerializeAsString();

    // call server
    _rpcClient.Call(RPCMethod::EchoTest, serializedArg, serializedResult);

    // parse result
    result.ParseFromString(serializedResult);
    echoText = result.text();
  }

  bool GroupClient::RegisterPartition(DBPartition& p) {
    PbPartition arg;
    std::string serializedArg;
    PbRpcGroupServiceResult result;
    std::string serializedResult;

    // prepare argument
    arg.set_name(p.Name);
    arg.set_publicport(p.PublicPort);
    arg.set_partitionport(p.PartitionPort);
    arg.set_replicationport(p.ReplicationPort);
    arg.set_partitionid(p.PartitionId);
    arg.set_replicaid(p.ReplicaId);
    serializedArg = arg.SerializeAsString();

    // call server
    _rpcClient.Call(RPCMethod::RegisterPartition, serializedArg, serializedResult);

    // parse result
    result.ParseFromString(serializedResult);

    return result.succeeded();
  }

  std::vector<std::vector<DBPartition>> GroupClient::GetRegisteredPartitions() {
    std::string emptyArg;
    PbRegisteredPartitions result;
    std::string serializedResult;
    std::vector<std::vector < DBPartition>> partitions;

    // call server
    _rpcClient.Call(RPCMethod::GetRegisteredPartitions, emptyArg, serializedResult);

    // parse result
    result.ParseFromString(serializedResult);
    int numPartitions = result.numpartitions();
    int numReplicasPerPartition = result.numreplicasperpartition();

    partitions.resize(numPartitions);
    for (int i = 0; i < numPartitions; i++) {
      for (int j = 0; j < numReplicasPerPartition; j++) {
        PbPartition rp = result.partition(i * numReplicasPerPartition + j);
        DBPartition p(rp.name(), rp.publicport(), rp.partitionport(),
          rp.replicationport(), rp.partitionid(), rp.replicaid());
        partitions[i].push_back(p);
      }
    }

    return partitions;
  }

  bool GroupClient::NotifyReadiness(DBPartition& p) {
    PbPartition arg;
    std::string serializedArg;
    PbRpcGroupServiceResult result;
    std::string serializedResult;

    // prepare argument
    arg.set_name(p.Name);
    arg.set_publicport(p.PublicPort);
    arg.set_partitionport(p.PartitionPort);
    arg.set_replicationport(p.ReplicationPort);
    arg.set_partitionid(p.PartitionId);
    arg.set_replicaid(p.ReplicaId);
    serializedArg = arg.SerializeAsString();

    // call server
    _rpcClient.Call(RPCMethod::NotifyReadiness, serializedArg, serializedResult);

    // parse result
    result.ParseFromString(serializedResult);

    return result.succeeded();
  }

} // namespace scc
