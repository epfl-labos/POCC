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


#include "kvservice/kv_server.h"
#include "common/sys_config.h"
#include "common/utils.h"
#include "common/types.h"
#include <stdlib.h>
#include <iostream>

using namespace scc;

int main(int argc, char *argv[]) {

    fprintf(stdout, "KVServerProgram!\n");
    if (argc == 6) {
        SysConfig::Consistency = Utils::str2consistency(argv[1]);
        std::string serverName = argv[2];
        int publicPort = atoi(argv[3]);
        int totalNumKeys = atoi(argv[4]);
        std::string duraStr = argv[5];
        if (duraStr == "Disk") {
            SysConfig::DurabilityOption = DurabilityType::Disk;
        } else if (duraStr == "Memory") {
            SysConfig::DurabilityOption = DurabilityType::Memory;
        } else {
            fprintf(stdout, "<Durability: Disk/Memory>\n");
            exit(1);
        }

        // start server
        KVServer server(serverName, publicPort, totalNumKeys);
        server.Run();

    } else if (argc == 14 || argc == 15 || argc == 18 || argc == 20) {
        SysConfig::Consistency = Utils::str2consistency(argv[1]);
        std::string serverName = argv[2];
        int publicPort = atoi(argv[3]);
        int partitionPort = atoi(argv[4]);
        int replicationPort = atoi(argv[5]);
        int partitionId = atoi(argv[6]);
        int replicaId = atoi(argv[7]);
        int totalNumKeys = atoi(argv[8]);
        std::string duraStr = argv[9];

        if (duraStr == "Disk") {
            SysConfig::DurabilityOption = DurabilityType::Disk;
        } else if (duraStr == "Memory") {
            SysConfig::DurabilityOption = DurabilityType::Memory;
        } else {
            fprintf(stdout, "<Durability: Disk/Memory>\n");
            exit(1);
        }
        std::string groupServerName = argv[10];
        int groupServerPort = atoi(argv[11]);

        SysConfig::OptimisticMode = (strcmp(argv[12], "true") == 0);
        if (SysConfig::OptimisticMode) cout << "OPTIMISTIC" << endl;
        else cout << "PESIMISTIC" << endl;

        std::string gstStr = argv[13];
        if (gstStr == "tree") {
            SysConfig::GSTDerivationMode = GSTDerivationType::TREE;
            cout << "TREE" << endl;
        } else if (gstStr == "broadcast") {
            SysConfig::GSTDerivationMode = GSTDerivationType::BROADCAST;
            cout << "BROADCAST" << endl;
        } else if (gstStr == "simple_broadcast") {
            SysConfig::GSTDerivationMode = GSTDerivationType::SIMPLE_BROADCAST;
            cout << "SIMPLE_BROADCAST" << endl;
        } else {
            fprintf(stdout, "<GSTDerivationMode: tree/broadcast/simple_broadcast>\n");
            exit(1);
        }

        // if (argc == 15 || argc == 18) {
        SysConfig::GSTComputationInterval = atoi(argv[14]);
        //}

        //if (argc == 18) {
        SysConfig::UpdatePropagationBatchTime = atoi(argv[15]);
        SysConfig::ReplicationHeartbeatInterval = atoi(argv[16]);
        SysConfig::WaitConditionInOptimisticSetOperation = (strcmp(argv[17], "true") == 0);

        //}

        //if (argc >= 19) {
        SysConfig::GetSpinTime = atoi(argv[18]);
        fprintf(stdout, "Spin time is %d\n", SysConfig::GetSpinTime);
        //}
        //if(argc>=20){
        SysConfig::OpttxDelta = atoi(argv[19]);
        fprintf(stdout, "OptTxDelta %d\n", SysConfig::OpttxDelta);
        //}
        // start server
        KVServer server(serverName, publicPort, partitionPort, replicationPort,
                        partitionId, replicaId, totalNumKeys,
                        groupServerName, groupServerPort);
        fprintf(stdout, "KVServerProgram:RUN!\n");
        server.Run();

    } else {
        fprintf(stdout, "Usage: %s <Causal> <KVServerName> <PublicPort> <TotalNumKeys> <Durability: Disk/Memory>\n",
                argv[0]);
        fprintf(stdout,
                "Usage: %s <Causal> <KVServerName> <PublicPort> <PartitionPort> <ReplicationPort> <PartitionId> <ReplicaId> <TotalNumKeys> <Durability: Disk/Memory> <GroupServerName> <GroupServerPort> <OptimisticMode> <GSTDerivationMode: tree/broadcast>\n",
                argv[0]);
        fprintf(stdout,
                "Usage: %s <Causal> <KVServerName> <PublicPort> <PartitionPort> <ReplicationPort> <PartitionId> <ReplicaId> <TotalNumKeys> <Durability: Disk/Memory> <GroupServerName> <GroupServerPort> <OptimisticMode> <GSTDerivationMode: tree/broadcast> <GSTInterval (us)>\n",
                argv[0]);
        fprintf(stdout,
                "Usage: %s <Causal> <KVServerName> <PublicPort> <PartitionPort> <ReplicationPort> <PartitionId> <ReplicaId> <TotalNumKeys> <Durability: Disk/Memory> <GroupServerName> <GroupServerPort> <OptimisticMode> <GSTDerivationMode: tree/broadcast> <GSTInterval (us)> <UpdatePropagationBatchTime (us)> <ReplicationHeartbeatInterval (us)> <WaitConditionInSetOperation (true/false)>\n",
                argv[0]);
        exit(1);
    }
}
