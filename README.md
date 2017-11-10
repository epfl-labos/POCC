#POCC
##What is it?

Optimistic Causal Consistency (OCC) is a novel approach of causal consistency described in the paper "Optimistic Causal Consistency for Geo-Replicated Key-Value Stores", presented at ICDCS'17 (http://ieeexplore.ieee.org/document/7980260/).

POCC is an implementation of OCC based on physical clocks.

You can find an extended version of the paper at:
https://infoscience.epfl.ch/record/225991/files/SDZ-POCC-infoscience.pdf

##Compilation
In order to compile this project\'s code, you need to install gcc4.8, protobuf-2.6.2, boost-1.63.0 and gtest-1.7.0.
The default project and build directories are /pocc and /pocc/build. You can change them by changing the PROJECT and BUILD values in the Makefile.
The code is then compiled simply by running twice:

```
    $make all
```
##Running POCC
In order to run POCC, you need to run the group manager, the server and client programs.

The group manager program can be run with:

```
    $/pocc/build/group_server_program <Port> <NumPartitions> <NumReplicasPerPartition>
```
 
A server program can be run with:

```
    $/pocc/build/kv_server_program <Causal> <KVServerName> <PublicPort> <PartitionPort> <ReplicationPort> <PartitionId> <ReplicaId> <TotalNumKeys> <Durability: Memory> <GroupServerName> <GroupServerPort> <OptimisticMode> <GSTDerivationMode: tree> <GSTInterval (us)> <UpdatePropagationBatchTime (us)> <ReplicationHeartbeatInterval (us)> <WaitConditionInSetOperation (true/false)> <SpinTime: default 500 (us)> <Delta: default 0 (us)>"
```
              
A client program can be run with:

```
    $/pocc/build/run_experiments <Causal> <RequestDistribution: zipfian/uniform> <RequestDistributionParameter> <ReadRatio> <WriteRatio> <ManagerName> <ManagerPort> <TotalNumItems> <WriteValueSize> <NumOpsPerThread: default 0> <NumThreads> <ServingReplicaId> <ClientServerId> <LatencyOutputFileName> <ReservoirSampling: default false> <ReservoirSamplingLimit> <ExperimentDuration: (ms)> <ClientSessionResetLimit> <EnableClientSessionResetLimit: default false> <NumTxReadItems> <NumTxReadItems> <Locality: default -1> <ThinkTime: default 0 (ms)>\n",
```

Additionally, you can run an interactive client program with:

```
    $/pocc/build/interactive_kv_client_program <Causal> <ServerName> <ServerPort>
```
    
and provide it with the command, or
```
    $/pocc/build/interactive_kv_client_program <Causal> <ServerName> <ServerPort> <Command>
```

where 'Command' can be one of: 

- Echo
- Get key
- Set key value
- ShowItem
- ShowState

##Licensing
**POCC is released under the Apache Licence, Version 2.0.** 

Please see the LICENSE file.
                                
##Contact
- Kristina Spirovska <kristina.spirovska@epfl.ch>
- Diego Didona <diego.didona@epfl.ch>

##Contributors
- Kristina Spirovska
- Diego Didona
- Jiaqing Du
- Calin Iorgulescu
- Amitabha Roy
- Sameh Elnikety







