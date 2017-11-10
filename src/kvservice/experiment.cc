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


#include "kvservice/experiment.h"
#include "common/sys_logger.h"
#include "experiment.h"
#include <boost/format.hpp>


namespace scc {

    double IntToDouble(int integer) {
        if (integer == 100 || integer == 75 || integer == 50 || integer == 25)
            return ((double) integer) / 100.0;
        if (integer == 12) // 1/8
            return 12.5 / 100.0;
        if (integer == 6) // 1/16
            return 6.25 / 100.0;
        if (integer == 3) // 1/32
            return 3.125 / 100.0;
        if (integer == 1)
            return 1.5625 / 100.0; // 1/64
        fprintf(stderr, "Integer not valid %d\n", integer);
        assert(false);
    }

    std::string GetGlobalRandomKey(int totalNumItems) {
        return std::to_string(abs(random()) % totalNumItems);
    }

    std::string GetKey(Generator<uint64_t> *generator) {
        int key = generator->Next();
        return std::to_string(key);
    }

    std::string GetRandomKeyAtPartition(int partitionId, int numPartitions, int totalNumItems) {
        int key;

        do {
            key = abs(random()) % (totalNumItems * numPartitions);
        } while (key % numPartitions != partitionId);

        return std::to_string(key);
    }

    std::string GetKeyAtPartition(Generator<uint64_t> *generator, int partitionId, int numPartitions) {
        int rand, key;

        rand = generator->Next();
        key = rand * numPartitions + partitionId;

        return std::to_string(key);
    }

    std::string GetKeyAtPartitionAtReplica(Generator<uint64_t> *generator, int partitionId, int numPartitions,
                                           int replicaId, int numLocalKeysPerReplica) {
        int rand, key;
        rand = generator->Next();//rand-th element of the replicaId-th slice of a partition
        rand += (replicaId *numLocalKeysPerReplica); //Sum an offset to obtain the index of the desired key in the desired partition
        key = rand * numPartitions + partitionId; //Now compute the absolute value of the key, that takes into account the hashing function

        return std::to_string(key);
    }

    void experiment1(ThreadArg *arg) {
        try {

            srand(Utils::GetThreadId() * time(0));

            std::string setValue(arg->NumValueBytes, 'y');
            std::string getValue;

            // connect to kv server
            for (int i = 0; i < arg->NumPartitions; ++i) {
                arg->Clients.push_back(new PublicClientLB(arg->ServerNames[i], arg->ServerPorts[i]));
            }

            // notify readiness
            arg->OpReadyEvent.Set();

            // wait to start
            arg->OpStartEvent.WaitAndReset();

            vector<int> partition_ids;
            for (int i = 0; i < arg->NumPartitions; ++i) {
                partition_ids.push_back(i);
            }

            int offsetR = 0;
            int offsetW = 0;

            while (!(*arg->stopOperation)) {

                random_shuffle(partition_ids.begin(), partition_ids.end());

                //read from every partition once
                for (int p_idx = 0; p_idx < arg->NumPartitions; p_idx++) {

                    //randomly select a key (according to a predefined key distribution)
                    std::string opKey = GetKeyAtPartition(arg->generator, partition_ids.at(p_idx), arg->NumPartitions);

                    //record operation start time
                    PhysicalTimeSpec startTimeOfGet = Utils::GetCurrentClockTime();

                    //GET key
                    bool ret = arg->Clients[partition_ids.at(p_idx)]->Get(opKey, getValue);

                    if (!ret) {
                        fprintf(stdout, "Get %s failed\n", opKey.c_str());
                        fflush(stdout);
                    }

                    //record operation end time
                    auto endTimeGet = std::chrono::high_resolution_clock::now();
                    PhysicalTimeSpec endTimeOfGet = Utils::GetCurrentClockTime();
                    arg->NumOpsPerThread += 1;


                    //record operation latency
                    double duration = (endTimeOfGet - startTimeOfGet).toMilliSeconds();

#ifdef RESERVOIR_SAMPLING

                    if (arg->OpReadLatencies.size() < arg->ReservoirSamplingLimit) {

                        arg->OpReadLatencies.push_back(duration);
                    }
                    else {
                        offsetR++;
                        int position = abs(random()) % (arg->ReservoirSamplingLimit + offsetR);
                        if (position < arg->ReservoirSamplingLimit) {
                            arg->OpReadLatencies[position] = duration;
                        }
                    }
#else
                    arg->OpReadLatencies.push_back(duration);
#endif
                }

                //randomly select a key (according to a predefined key distribution)
                std::string opKey;
                opKey = GetKeyAtPartition(arg->generator, arg->LocalPartitionId, arg->NumPartitions);

                //record operation start time
                PhysicalTimeSpec startTimeOfSet = Utils::GetCurrentClockTime();

                //write to the local partition: SET key
                bool ret = arg->Clients[arg->LocalPartitionId]->Set(opKey, setValue);

                if (!ret) {
                    fprintf(stdout, "Set %s failed\n", opKey.c_str());
                    fflush(stdout);
                }

                //record operation end time
                PhysicalTimeSpec endTimeOfSet = Utils::GetCurrentClockTime();

                arg->NumOpsPerThread += 1;

                //record operation latency
                double duration = (endTimeOfSet - startTimeOfSet).toMilliSeconds();

#ifdef RESERVOIR_SAMPLING

                if (arg->OpWriteLatencies.size() < arg->ReservoirSamplingLimit) {
                    arg->OpWriteLatencies.push_back(duration);
                }
                else {
                    offsetW++;
                    int position = abs(random()) % (arg->ReservoirSamplingLimit + offsetW);
                    if (position < arg->ReservoirSamplingLimit) {
                        arg->OpWriteLatencies[position] = duration;
                    }
                }
#else
                arg->OpWriteLatencies.push_back(duration);
#endif
            }

            // notify stopping
            arg->OpEndEvent.Set();
        } catch (
                SocketException &e
        ) {
            fprintf(stdout, "SocketException: %s\n", e.what());
            exit(1);
        } catch (...) {
            fprintf(stdout, "Exception while executing Experiment 1. ");
            exit(1);
        }
    }

    void experiment2(ThreadArg *arg) {
        //Round Robin R/W ration
        try {
            srand(Utils::GetThreadId() * time(0));

            // connect to kv server
            std::string setValue(arg->NumValueBytes, 'y');
            std::string getValue;

            std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 50 + 1));
            arg->Client = new PublicClientLB(arg->ServerNames[arg->servingPartition],
                                             arg->ServerPorts[arg->servingPartition]);

            arg->Client->setTotalNumKeyInKVStore(arg->TotalNumItems);

            // notify readiness
            arg->OpReadyEvent.Set();

            // wait to start
            arg->OpStartEvent.WaitAndReset();

            int offsetR = 0;
            int offsetW = 0;
            int numCycles = 0;

            while (!(*arg->stopOperation)) {

                int pId = rand() % arg->NumPartitions;

                // READ OPERATIONS
                for (int rCount = 0; rCount < arg->ReadRatio; rCount++) {
                    //randomly select a key (according to a predefined key distribution)
                    std::this_thread::sleep_for(std::chrono::milliseconds(arg->thinkTime));
                    std::string opKey = GetKeyAtPartition(arg->generator, pId, arg->NumPartitions);

                    //record operation start time
                    PhysicalTimeSpec startTimeOfGet = Utils::GetCurrentClockTime();

                    //GET key
                    bool ret = arg->Client->Get(opKey, getValue);

                    if (!ret) {
                        fprintf(stdout, "Get %s failed\n", opKey.c_str());
                        fflush(stdout);
                    }

                    PhysicalTimeSpec endTimeOfGet = Utils::GetCurrentClockTime();
                    arg->NumOpsPerThread += 1;

                    //record operation latency
                    double duration = (endTimeOfGet - startTimeOfGet).toMilliSeconds();

                    arg->TotalNumReadOps += 1;
                    arg->ReadLatenciesSum += duration;

                    if (arg->MaxReadLatency < duration) {
                        arg->MaxReadLatency = duration;
                    }

                    if (arg->MinReadLatency > duration) {
                        arg->MinReadLatency = duration;
                    }

#ifdef RESERVOIR_SAMPLING

                    if (arg->OpReadLatencies.size() < arg->ReservoirSamplingLimit) {

                        arg->OpReadLatencies.push_back(duration);
                    }
                    else {
                        offsetR++;
                        int position = abs(random()) % (arg->ReservoirSamplingLimit + offsetR);
                        if (position < arg->ReservoirSamplingLimit) {
                            arg->OpReadLatencies[position] = duration;
                        }
                    }
#else
                    arg->OpReadLatencies.push_back(duration);
#endif
                    pId = (pId + 1) % arg->NumPartitions;

                }

                // WRITE OPERATIONS
                pId = rand() % arg->NumPartitions;
                for (int wCount = 0; wCount < arg->WriteRatio; wCount++) {
                    //randomly select a key (according to a predefined key distribution)
                    std::string opKey;
#ifdef THINK_TIME_BEFORE_WRITE
                    std::this_thread::sleep_for(std::chrono::milliseconds(arg->thinkTime));
#endif
                    opKey = GetKeyAtPartition(arg->generator, pId, arg->NumPartitions);
                    //record operation start time
                    PhysicalTimeSpec startTimeOfSet = Utils::GetCurrentClockTime();

                    //write to the local partition: SET key
                    bool ret = arg->Client->Set(opKey, setValue);

                    if (!ret) {
                        fprintf(stdout, "Set %s failed\n", opKey.c_str());
                        fflush(stdout);
                    }

                    //record operation end time
                    PhysicalTimeSpec endTimeOfSet = Utils::GetCurrentClockTime();
                    arg->NumOpsPerThread += 1;

#ifdef ENABLE_CLIENT_RESET
                    if (arg->enableClientReset) {
                        if (arg->clientResetNumber == numCycles) {
                            arg->Client->resetSession();
                            numCycles = 0;
                        }
                    }
#endif

                    //record operation latency
                    double duration = (endTimeOfSet - startTimeOfSet).toMilliSeconds();

                    arg->TotalNumWriteOps += 1;
                    arg->WriteLatenciesSum += duration;

                    if (arg->MaxWriteLatency < duration) {
                        arg->MaxWriteLatency = duration;
                    }

                    if (arg->MinWriteLatency > duration) {
                        arg->MinWriteLatency = duration;
                    }

#ifdef RESERVOIR_SAMPLING


                    if (arg->OpWriteLatencies.size() < arg->ReservoirSamplingLimit) {
                        arg->OpWriteLatencies.push_back(duration);
                    }
                    else {
                        offsetW++;
                        int position = abs(random()) % (arg->ReservoirSamplingLimit + offsetW);
                        if (position < arg->ReservoirSamplingLimit) {
                            arg->OpWriteLatencies[position] = duration;
                        }
                    }
#else
                    arg->OpWriteLatencies.push_back(duration);

#endif
                    pId = (pId + 1) % arg->NumPartitions;
                }
            }

            arg->numHotBlockKeys = 0;

            // notify stopping
            arg->OpEndEvent.Set();
        } catch (SocketException &e) {
            fprintf(stdout, "SocketException: %s\n", e.what());
            exit(1);
        } catch (...) {
            fprintf(stdout, "Exception while executing Experiment 1. ");
            exit(1);
        }
    }

    static int _partitionsToReadFrom = -1;

    void experimentTx(ThreadArg *arg) {

        try {
            srand(Utils::GetThreadId() * time(0));
            std::this_thread::sleep_for(std::chrono::milliseconds(10 * arg->ThreadId));

            // connect to kv server
            PublicClientLB client(arg->ServerNames[arg->servingPartition], arg->ServerPorts[arg->servingPartition]);

            client.setTotalNumKeyInKVStore(arg->TotalNumItems);

            // notify readiness
            arg->OpReadyEvent.Set();

            // wait to start
            arg->OpStartEvent.WaitAndReset();

            int p_idx = 0;
            vector<int> partition_ids;
            for (int i = 0; i < arg->NumPartitions; ++i) {
                partition_ids.push_back(i);
            }

            srand(time(0));
            random_shuffle(partition_ids.begin(), partition_ids.end());

            std::vector<std::string> txGetKeys;
            std::vector<std::string> txGetValues;

            const int partitionsToReadFrom = (int) (IntToDouble(arg->ReadRatio) *
                                                    ((double) partition_ids.size())) > 0 ?
                                             (int) ((IntToDouble(arg->ReadRatio) * ((double) partition_ids.size())))
                                                                                         : 1;
            const int numXactPerIteration = arg->NumTxReadItems;

            fprintf(stdout, "PartitionsToReadFrom %d\n", partitionsToReadFrom);
            fprintf(stdout, "NumXactPerXact %d\n", numXactPerIteration);

            if (arg->ThreadId == 0)
                _partitionsToReadFrom = partitionsToReadFrom;

            int pId;
            bool ret;

            while (!(*arg->stopOperation)) {
                for (int j = 0; j < numXactPerIteration; j++) {
                    // ====== do some transactions ========

                    txGetKeys.clear();
                    txGetValues.clear();

                    pId = rand() % arg->NumPartitions;

                    // READ OPERATIONS

                    for (int rCount = 0; rCount < partitionsToReadFrom; rCount++) {
                        std::string opKey = GetKeyAtPartition(arg->generator, pId, arg->NumPartitions);
                        txGetKeys.push_back(opKey);
                        pId = (pId + 1) % arg->NumPartitions;
                    }

                    //record operation start time
                    PhysicalTimeSpec startTime = Utils::GetCurrentClockTime();
                    std::this_thread::sleep_for(std::chrono::milliseconds(arg->thinkTime));
                    ret = client.TxGet(txGetKeys, txGetValues);
                    if (!ret) {
                        fprintf(stdout, "TxGet failed\n");
                    }

                    PhysicalTimeSpec endTime = Utils::GetCurrentClockTime();
                    double duration = (endTime - startTime).toMilliSeconds();

                    arg->NumOpsPerThread += 1;
                    arg->TotalNumReadOps += 1;

                    arg->ReadLatenciesSum += duration;

                    if (arg->MaxReadLatency < duration) {
                        arg->MaxReadLatency = duration;
                    }

                    if (arg->MinReadLatency > duration) {
                        arg->MinReadLatency = duration;
                    }

#ifdef RESERVOIR_SAMPLING

                    if (arg->OpReadLatencies.size() < arg->ReservoirSamplingLimit) {

                            arg->OpReadLatencies.push_back(duration);
                        }
                        else {
                            offsetR++;
                            int position = abs(random()) % (arg->ReservoirSamplingLimit + offsetR);
                            if (position < arg->ReservoirSamplingLimit) {
                                arg->OpReadLatencies[position] = duration;
                            }
                        }
#else
                    arg->OpReadLatencies.push_back(duration);
#endif
                }

                // ====== do a write ========

                std::string setValue(arg->NumValueBytes, 'y');
                std::string opKey;
                std::string getValue;

                pId = rand() % arg->NumPartitions;
                for (int wCount = 0; wCount < arg->WriteRatio; wCount++) {

                    opKey = GetKeyAtPartition(arg->generator, pId, arg->NumPartitions);

                    //record operation start time
                    PhysicalTimeSpec startTimeOfSet = Utils::GetCurrentClockTime();

                    //write to the local partition: SET key
                    std::this_thread::sleep_for(std::chrono::milliseconds(arg->thinkTime));
                    ret = client.Set(opKey, setValue);

                    if (!ret) {
                        fprintf(stdout, "Set %s failed\n", opKey.c_str());
                        fflush(stdout);
                    }

                    //record operation end time
                    PhysicalTimeSpec endTimeOfSet = Utils::GetCurrentClockTime();

                    //record operation latency
                    double durationWrite = (endTimeOfSet - startTimeOfSet).toMilliSeconds();

                    arg->TotalNumWriteOps += 1;
                    arg->NumOpsPerThread += 1;
                    arg->WriteLatenciesSum += durationWrite;

                    if (arg->MaxWriteLatency < durationWrite) {
                        arg->MaxWriteLatency = durationWrite;
                    }

                    if (arg->MinWriteLatency > durationWrite) {
                        arg->MinWriteLatency = durationWrite;
                    }
#ifdef RESERVOIR_SAMPLING
                    if (arg->OpWriteLatencies.size() < arg->ReservoirSamplingLimit) {
                        arg->OpWriteLatencies.push_back(durationWrite);
                    }
                    else {
                        offsetW++;
                        int position = abs(random()) % (arg->ReservoirSamplingLimit + offsetW);
                        if (position < arg->ReservoirSamplingLimit) {
                            arg->OpWriteLatencies[position] = durationWrite;
                        }
                    }
#else
                    arg->OpWriteLatencies.push_back(durationWrite);

#endif
                    pId = (pId + 1) % arg->NumPartitions;
                }

                std::this_thread::sleep_for(std::chrono::milliseconds(arg->thinkTime));

            } // end while

            arg->numHotBlockKeys = 0;

            // notify stopping
            arg->OpEndEvent.Set();

        } catch (SocketException &e) {
            fprintf(stdout, "SocketException: %s\n", e.what());
            exit(1);
        }
    }

    void experiment4(ThreadArg *arg) {

        //Round Robin R/W ration
        try {

            srand(Utils::GetThreadId() * time(0));

            // connect to kv server
            std::string setValue(arg->NumValueBytes, 'y');
            std::string getValue;


            arg->Client = new PublicClientLB(arg->ServerNames[arg->servingPartition],
                                             arg->ServerPorts[arg->servingPartition]);

            arg->Client->setTotalNumKeyInKVStore(arg->TotalNumItems);

            // notify readiness
            arg->OpReadyEvent.Set();

            // wait to start
            arg->OpStartEvent.WaitAndReset();

            int offsetR = 0;
            int offsetW = 0;
            int numCycles = 0;

            /////////// GENERATE LOCALITY AWARE DATA STRUCTURES /////////////
            std::vector<int> dc_vector;
            int local = arg->locality; //Percentage or gets issued towards local data
            fflush(stdout);
            int local_replica_id = arg->Client->getLocalReplicaId();

            int otherReplicas = arg->Client->getNumReplicas() - 1;
            assert((100 - local) % otherReplicas == 0); //We want nice numbers :)

            int otherReplicasBuckets =
                    (100 - local) / otherReplicas; //Percentage of get towards a specific remote partition

            //Set the local entries
            for (int i = 0; i < local; i++) {
                dc_vector.push_back(local_replica_id);
            }
            int remote_index = local;
            //Set the remote entries
            for (int i = 0; i < arg->Client->getNumReplicas(); i++) {
                if (i != local_replica_id) {
                    for (int j = 0; j < otherReplicasBuckets; j++) {
                        dc_vector.push_back(i);
                        remote_index++;
                    }
                }
            }

            //Make sure the vector is differently shuffled across threads
            for (int i = 0; i < arg->ThreadId * (local_replica_id + 1); i++) {
                std::random_shuffle(dc_vector.begin(), dc_vector.end());
            }

            //Drawing uniformly from dc_generator you have a local% probability of hitting the local DC
            //The remainder is equally split among remote DCs
            UniformGenerator dc_generator(0, 99);

            int next_dc;
            //Number of keys belonging to a replica per partition. Needed to compute the offset to obtain the per-partition
            //key id starting from the per-DC key id within that partition.
            int local_keys_per_replca = (arg->TotalNumItems / arg->NumPartitions) / arg->NumReplicas;


            /////////// START THE TEST /////////////

            while (!(*arg->stopOperation)) {

                int pId = rand() % arg->NumPartitions;

                // READ OPERATIONS
                for (int rCount = 0; rCount < arg->ReadRatio; rCount++) {
                    //randomly select a key (according to a predefined key distribution)
                    next_dc = dc_vector[dc_generator.Next()];
                    std::string opKey = GetKeyAtPartitionAtReplica(arg->generator, pId, arg->NumPartitions, next_dc,
                                                                   local_keys_per_replca);

                    //record operation start time
                    PhysicalTimeSpec startTimeOfGet = Utils::GetCurrentClockTime();

                    //GET key
                    fflush(stdout);
                    bool ret = arg->Client->Get(opKey, getValue);

                    if (!ret) {
                        fprintf(stdout, "Get %s failed\n", opKey.c_str());
                        fflush(stdout);
                    }

                    PhysicalTimeSpec endTimeOfGet = Utils::GetCurrentClockTime();
                    arg->NumOpsPerThread += 1;

                    //record operation latency
                    double duration = (endTimeOfGet - startTimeOfGet).toMilliSeconds();

                    arg->TotalNumReadOps += 1;
                    arg->ReadLatenciesSum += duration;

                    if (arg->MaxReadLatency < duration) {
                        arg->MaxReadLatency = duration;
                    }

                    if (arg->MinReadLatency > duration) {
                        arg->MinReadLatency = duration;
                    }

#ifdef RESERVOIR_SAMPLING
                    if (arg->OpReadLatencies.size() < arg->ReservoirSamplingLimit) {

                        arg->OpReadLatencies.push_back(duration);
                    }
                    else {
                        offsetR++;
                        int position = abs(random()) % (arg->ReservoirSamplingLimit + offsetR);
                        if (position < arg->ReservoirSamplingLimit) {
                            arg->OpReadLatencies[position] = duration;
                        }
                    }
#else
                    arg->OpReadLatencies.push_back(duration);
#endif
                    pId = (pId + 1) % arg->NumPartitions;

                }

                // WRITE OPERATIONS
                pId = rand() % arg->NumPartitions;
                for (int wCount = 0; wCount < arg->WriteRatio; wCount++) {
                    //randomly select a key (according to a predefined key distribution)

                    //Writes are always local
                    std::string opKey = GetKeyAtPartitionAtReplica(arg->generator, pId,
                                                                   arg->NumPartitions,
                                                                   local_replica_id, local_keys_per_replca);
                    //record operation start time
                    PhysicalTimeSpec startTimeOfSet = Utils::GetCurrentClockTime();

                    //write to the local partition: SET key
                    bool ret = arg->Client->Set(opKey, setValue);

                    if (!ret) {
                        fprintf(stdout, "Set %s failed\n", opKey.c_str());
                        fflush(stdout);
                    }

                    //record operation end time
                    PhysicalTimeSpec endTimeOfSet = Utils::GetCurrentClockTime();

                    arg->NumOpsPerThread += 1;

#ifdef ENABLE_CLIENT_RESET
                    if (arg->enableClientReset) {
                        if (arg->clientResetNumber == numCycles) {
                            arg->Client->resetSession();
                            numCycles = 0;
                        }
                    }
#endif

                    //record operation latency
                    double duration = (endTimeOfSet - startTimeOfSet).toMilliSeconds();

                    arg->TotalNumWriteOps += 1;
                    arg->WriteLatenciesSum += duration;

                    if (arg->MaxWriteLatency < duration) {
                        arg->MaxWriteLatency = duration;
                    }

                    if (arg->MinWriteLatency > duration) {
                        arg->MinWriteLatency = duration;
                    }

#ifdef RESERVOIR_SAMPLING


                    if (arg->OpWriteLatencies.size() < arg->ReservoirSamplingLimit) {
                        arg->OpWriteLatencies.push_back(duration);
                    }
                    else {
                        offsetW++;
                        int position = abs(random()) % (arg->ReservoirSamplingLimit + offsetW);
                        if (position < arg->ReservoirSamplingLimit) {
                            arg->OpWriteLatencies[position] = duration;
                        }
                    }
#else
                    arg->OpWriteLatencies.push_back(duration);

#endif
                    pId = (pId + 1) % arg->NumPartitions;
                }


            }

            arg->numHotBlockKeys = 0;

            // notify stopping
            arg->OpEndEvent.Set();
        } catch (SocketException &e) {
            fprintf(stdout, "SocketException: %s\n", e.what());
            exit(1);
        } catch (...) {
            fprintf(stdout, "Exception while executing Experiment 1. ");
            exit(1);
        }
    }

    Experiment::Experiment(char *argv[]) {

        this->stopOperation = false;

        this->config.RequestsDistribution = argv[2];
        this->config.RequestsDistributionParameter = atof(argv[3]);
        this->config.ReadRatio = atoi(argv[4]);
        this->config.WriteRatio = atoi(argv[5]);
        this->config.GroupServerName = argv[6];
        this->config.GroupServerPort = atoi(argv[7]);
        GroupClient groupClient(config.GroupServerName, config.GroupServerPort);
        allPartitions = groupClient.GetRegisteredPartitions();
        this->config.NumPartitions = allPartitions.size();
        this->config.NumReplicasPerPartition = allPartitions[0].size();
        this->config.TotalNumItems = atoi(argv[8]);
        this->config.NumValueBytes = atoi(argv[9]);
//        this->config.NumOpsPerThread = atoi(argv[10]);
        this->config.NumOpsPerThread = 0;
        this->config.TotalNumOps = 0;
        this->config.NumThreads = atoi(argv[11]);

        this->config.ServingPartitionId = atoi(argv[12]);
        this->config.ServingReplicaId = atoi(argv[13]);
        this->config.clientServerName = argv[14];
        this->config.latencyOutputFileName = argv[15];
        this->config.reservoirSampling = (strcmp(argv[16], "true") == 0);
        this->config.reservoirSamplingLimit = atoi(argv[17]);
        this->config.experimentDuration = atof(argv[18]);
        this->config.experimentType = atoi(argv[19]);
        this->config.clientResetNumber = atoi(argv[20]);
        this->config.enableClientReset = (strcmp(argv[21], "true") == 0);
        this->config.numTxReadItems = atoi(argv[22]);
        fprintf(stdout, "numTxReadItems %d\n", this->config.numTxReadItems);
        this->config.locality = atoi(argv[23]);
        this->config.thinkTime = atoi(argv[24]);

        printExperimentParameters();
    }

    void Experiment::printExperimentParameters() {
        fprintf(stdout, "##EXPERIMENT\n");
        fprintf(stdout, "##EXPERIMENT_TYPE %d \n", config.experimentType);
        fprintf(stdout, "##REQUEST_DISTRIBUTION %s \n", config.RequestsDistribution.c_str());
        fprintf(stdout, "##REQUEST_DISTRIBUTION_PARAM %lf \n", config.RequestsDistributionParameter);
        fprintf(stdout, "##GROUPSERVER_NAME %s \n", config.GroupServerName.c_str());
        fprintf(stdout, "##GROUPSERVER_PORT %d \n", config.GroupServerPort);
        fprintf(stdout, "##NUM_PARTITIONS %d \n", config.NumPartitions);
        fprintf(stdout, "##NUM_REPLICAS_PER_PARTITION %d \n", config.NumReplicasPerPartition);
        fprintf(stdout, "##TOTAL_NUM_ITEMS %d \n", config.TotalNumItems);
        fprintf(stdout, "##NUM_VALUE_BYTES %d \n", config.NumValueBytes);
        fprintf(stdout, "##NUM_OPS_PER_THERAD %d \n", config.NumOpsPerThread);
        fprintf(stdout, "##NUM_THREADS %d \n", config.NumThreads);
        fprintf(stdout, "##SERVING_PARTITION_ID %d \n", config.ServingPartitionId);
        fprintf(stdout, "##SERVING_REPLICA %d \n", config.ServingReplicaId);
        fprintf(stdout, "##CLIENT_SERVER_NAME %s \n", config.clientServerName.c_str());
        fprintf(stdout, "##LATENCY OUTPUT FILE NAME %s \n", config.latencyOutputFileName.c_str());
        fprintf(stdout, "##RESERVOIR SAMPLING %d \n", config.reservoirSampling);
        fprintf(stdout, "##RESERVOIR SAMPLING LIMIT %d \n", config.reservoirSamplingLimit);
        fprintf(stdout, "##EXPERIMENT DURATION %d \n", config.experimentDuration);
        fprintf(stdout, "##NUM_TxReadItems %d \n", config.numTxReadItems);
        fprintf(stdout, "##LOCALITY %d\n", config.locality);
        fprintf(stdout, "##THINK_TIME %d\n", config.thinkTime);

        fflush(stdout);
    }

    void Experiment::buildThreadArguments() {
        for (int i = 0; i < config.NumThreads; i++) {
            ThreadArg *arg = new ThreadArg;
            arg->ThreadId = i;

            int partitionIndex = config.ServingPartitionId;
            arg->LocalPartitionId = partitionIndex;
            int replicaIndex = config.ServingReplicaId;

            for (int k = 0; k < config.NumPartitions; ++k) {
                DBPartition p = allPartitions[k][replicaIndex];
                arg->ServerNames.push_back(p.Name);
                arg->ServerPorts.push_back(p.PublicPort);
            }

            arg->NumPartitions = config.NumPartitions;
            arg->NumReplicas = config.NumReplicasPerPartition;
            arg->TotalNumItems = config.TotalNumItems;

            arg->NumOpsPerThread = 0;
            arg->NumValueBytes = config.NumValueBytes;
            arg->ReadRatio = config.ReadRatio;
            arg->WriteRatio = config.WriteRatio;
            arg->ReservoirSampling = config.reservoirSampling;
            arg->ReservoirSamplingLimit = config.reservoirSamplingLimit;
            arg->stopOperation = &(this->stopOperation);

            arg->servingPartition = config.ServingPartitionId;
            arg->servingReplica = config.ServingReplicaId;
            arg->ReadRatio = config.ReadRatio;
            arg->WriteRatio = config.WriteRatio;
            arg->ReadLatenciesSum = 0;
            arg->WriteLatenciesSum = 0;
            arg->TotalNumReadOps = 0;
            arg->TotalNumWriteOps = 0;
            arg->MinReadLatency = 100000000;
            arg->MaxReadLatency = 0;
            arg->MinWriteLatency = 100000000;
            arg->MaxWriteLatency = 0;
            arg->numHotBlockKeys = 0;
            arg->clientResetNumber = config.clientResetNumber;
            arg->enableClientReset = config.enableClientReset;
            arg->NumTxReadItems = config.numTxReadItems;
            arg->RequestsDistribution = config.RequestsDistribution;
            arg->locality = config.locality;
            arg->thinkTime = config.thinkTime;
            initializeThreadRequestDistribution(arg);

            threadArgs.push_back(arg);
            opReadyEvents.push_back(&arg->OpReadyEvent);
            opEndEvents.push_back(&arg->OpEndEvent);
        }
    }

    void Experiment::initializeThreadRequestDistribution(ThreadArg *t) {

        int numItems;
        if (config.experimentType == 4)
            numItems = (config.TotalNumItems / config.NumPartitions) / config.NumReplicasPerPartition - 1;
        else
            numItems = config.TotalNumItems / config.NumPartitions - 1;

        if (t->RequestsDistribution == "uniform") {
            srand(Utils::GetThreadId() * time(0));
            std::this_thread::sleep_for(std::chrono::milliseconds(10 * rand() % 10));
            t->generator = new UniformGenerator(numItems);

        } else if (t->RequestsDistribution == "zipfian") {
            t->generator = new ZipfianGenerator(t->ThreadId, 0, numItems,
                                                config.RequestsDistributionParameter);

        } else if (t->RequestsDistribution == "scrambledZipfian") {
            t->generator = new ScrambledZipfianGenerator(numItems);
        } else if (config.RequestsDistribution == "skewLatest") {
            CounterGenerator insert_key_sequence_(0);
            insert_key_sequence_.Set(numItems + 1);
            t->generator = new SkewedLatestGenerator(t->ThreadId, insert_key_sequence_);
        } else if (t->RequestsDistribution == "hotSpot") {
            t->generator = new HotSpotGenerator(numItems,
                                                config.RequestsDistributionParameter, 0.01);
        } else {
            printf("Non existing distribution.");
        }
    }


    void Experiment::launchBenchmarkingClientThreads() {
        void (*func)(ThreadArg *arg);
        switch (config.experimentType) {
            case 1:
                func = &experiment1;
                break;
            case 2:
                func = &experiment2;
                break;
            case 3:
                func = &experimentTx;
                break;
            case 4:
                func = &experiment4;
                break;
            default:
                fprintf(stdout,
                        "[ERROR]: Unknown experiment type. The default experiment, experiment 1 will be executed.\n");
                func = &experiment1;
                break;
        }

        for (int i = 0; i < config.NumThreads; i++) {
            std::thread *t = new std::thread(func, this->threadArgs[i]);
            t->detach();
            threads.push_back(t);
        }

        fprintf(stdout, "[INFO]: Launched all threads.\n");

    }

    void Experiment::writeResults(FILE *&stream, Results results) {
        std::string p = std::to_string(results.NumPartitions);
        std::string r = std::to_string(results.NumReplicas);
        char separator = ';';


        std::string shortForm = (
                boost::format("*%s " //0
                                      "%d " //1
                                      "%.5lf " //3
                                      "%.5lf " //4
                                      "%.5lf " //5
                                      "%.5lf " //6
                                      "%.5lf " //7
                                      "%.5lf " //8
                                      "%.5lf " //9
                                      "%.5lf " //10

                                      "%.5lf " //11
                                      "%.5lf " //12
                                      "%.5lf " //13
                                      "%.5lf " //14
                                      "%.5lf " //15
                                      "%.5lf " //16
                                      "%.5lf " //17

                                      "%d " //18
                                      "%d " //19
                                      "%d " //20
                                      "%d " //21
                                      "%.5f " //22
                                      "%.5f\n\n") //23
                % config.clientServerName //0
                % config.ServingReplicaId //1
                % results.OpThroughput //3
                % results.OpGetLatencyStats.average //4
                % results.OpGetLatencyStats.median //5
                % results.OpGetLatencyStats._90Percentile //6
                % results.OpGetLatencyStats._95Percentile //7
                % results.OpGetLatencyStats._99Percentile //8
                % results.OpGetLatencyStats.min //9
                % results.OpGetLatencyStats.max //10

                % results.OpSetLatencyStats.average //11
                % results.OpSetLatencyStats.median //12
                % results.OpSetLatencyStats._90Percentile //13
                % results.OpSetLatencyStats._95Percentile //14
                % results.OpSetLatencyStats._99Percentile //15
                % results.OpSetLatencyStats.min //16
                % results.OpSetLatencyStats.max //17

                % results.NumPartitions //18
                % results.NumReplicas //19
                % results.NumThreadsPerReplica //20
                % results.NumOpsPerThread //21
                % results.TotalOps //22
                % results.TotalTime).str(); //23

        replace(shortForm.begin(), shortForm.end(), ' ', separator);

        cout << shortForm << endl;
        fprintf(stream, "Finished %.5lf operations in %.5lf milliseconds\n", results.TotalOps,
                results.TotalTime);
        fprintf(stream, "Operation throughput is %.5lf ops/ms\n", results.OpThroughput);
        fprintf(stream, "Operation average read latency is %.5lf milliseconds\n",
                results.OpGetLatencyStats.average);
        fprintf(stream, "Operation median read latency is %.5lf milliseconds\n",
                results.OpGetLatencyStats.median);
        fprintf(stream, "Operation 75-percentile read latency is %.5lf milliseconds\n",
                results.OpGetLatencyStats._75Percentile);
        fprintf(stream, "Operation 90-percentile read latency is %.5lf milliseconds\n",
                results.OpGetLatencyStats._90Percentile);
        fprintf(stream, "Operation 95-percentile read latency is %.5lf milliseconds\n",
                results.OpGetLatencyStats._95Percentile);
        fprintf(stream, "Operation 99-percentile read latency is %.5lf milliseconds\n",
                results.OpGetLatencyStats._99Percentile);
        fprintf(stream, "Operation minimum observed read latency is %.5lf milliseconds\n",
                results.OpGetLatencyStats.min);
        fprintf(stream, "Operation maximum observed read latency is %.5lf milliseconds\n",
                results.OpGetLatencyStats.max);
        fprintf(stream, "Operation read latency variance is %.5lf milliseconds\n",
                results.OpGetLatencyStats.variance);
        fprintf(stream, "Operation read latency standard deviation is %.5lf milliseconds\n",
                results.OpGetLatencyStats.standardDeviation);

        fprintf(stream, "Operation average write latency is %.5lf milliseconds\n",
                results.OpSetLatencyStats.average);
        fprintf(stream, "Operation median write latency is %.5lf milliseconds\n",
                results.OpSetLatencyStats.median);
        fprintf(stream, "Operation 75-percentile write latency is %.5lf milliseconds\n",
                results.OpSetLatencyStats._75Percentile);
        fprintf(stream, "Operation 90-percentile write latency is %.5lf milliseconds\n",
                results.OpSetLatencyStats._90Percentile);
        fprintf(stream, "Operation 95-percentile write latency is %.5lf milliseconds\n",
                results.OpSetLatencyStats._95Percentile);
        fprintf(stream, "Operation 99-percentile write latency is %.5lf milliseconds\n",
                results.OpSetLatencyStats._99Percentile);
        fprintf(stream, "Operation minimum observed write latency is %.5lf milliseconds\n",
                results.OpSetLatencyStats.min);
        fprintf(stream, "Operation maximum observed write latency is %.5lf milliseconds\n",
                results.OpSetLatencyStats.max);
        fprintf(stream, "Operation write latency variance is %.5lf milliseconds\n",
                results.OpSetLatencyStats.variance);
        fprintf(stream, "Operation write latency standard deviation is %.5lf milliseconds\n",
                results.OpSetLatencyStats.standardDeviation);
        fprintf(stream, "Number of hot blocked keys is %d \n",
                results.NumHotBlockedKeys);

#ifdef WRITE_LATENCY_DISTRIBUTION

        std::string fileName = config.latencyOutputFileName + "_read_latency.txt";

        fprintf(stream, "\n[INFO] Writing read latencies to %s_read_latency.txt \n",
                config.latencyOutputFileName.c_str());
        writeLatencyDistribution(fileName, results.readLatencies);

        fileName = config.latencyOutputFileName + "_write_latency.txt";
        fprintf(stream, "\n[INFO] Writing write latencies to %s_write_latency.txt \n",
                config.latencyOutputFileName.c_str());

        writeLatencyDistribution(fileName, results.writeLatencies);
#endif

#ifdef WAIT_REPLICATION
        fprintf(stream, "Wait on replication is true\n");
#else
        fprintf(stream, "Wait on replication is false\n");
#endif

#ifdef DEP_VECTORS
        fprintf(stream, "DEP_VECTORS is true\n");
#else
        fprintf(stream, "DEP_VECTORS is false\n");
#endif
        fprintf(stream, "PARTITIONS_TO_READ_FROM %d\n", _partitionsToReadFrom);

        fprintf(stream, "NUM_READS %f\n", results.TotalReadOps);
        fprintf(stream, "NUM_WRITES %f\n", results.TotalWriteOps);
        fprintf(stream, "sum_read_latency %f\n", results.OpGetLatencyStats.average * results.TotalReadOps);
        fprintf(stream, "sum_write_latency %f\n", results.OpSetLatencyStats.average * results.TotalWriteOps);
    }

    void Experiment::writeLatencyDistribution(std::string fileName, vector<double> vector) {

        std::sort(vector.begin(), vector.end());

        FILE *stream;
        stream = fopen(fileName.c_str(), "w");

        for (int i = 0; i < vector.size(); i++) {
            fprintf(stream, "%.5lf;", vector[i]);
        }
        fprintf(stream, "\n\n");

        fclose(stream);
    }


    void calculateLatencyMeasurements(std::vector<double> opLatencies, std::vector<double> &latencies,
                                      double &totalLatency,
                                      double &minLatency, double &maxLatency) {
        latencies.insert(latencies.end(), opLatencies.begin(), opLatencies.end());

        totalLatency += std::accumulate(opLatencies.begin(),
                                        opLatencies.end(), 0.0);

        double min = *(std::min_element(std::begin(opLatencies),
                                        std::end(opLatencies)));

        if (minLatency > min) {
            minLatency = min;
        }

        double max = *(std::max_element(std::begin(opLatencies),
                                        std::end(opLatencies)));
        if (maxLatency < max) {
            maxLatency = max;
        }

    }

    void Experiment::calculateMedian_Percentiles(std::vector<double> vect, Statistics &stat) {

        std::sort(vect.begin(), vect.end());

        stat.median = vect.at((int) std::round(vect.size() * 0.50));

        stat._75Percentile = vect.at((int) std::round(vect.size() * 0.75) - 1);

        stat._90Percentile = vect.at((int) std::round(vect.size() * 0.90) - 1);

        stat._95Percentile = vect.at((int) std::round(vect.size() * 0.95) - 1);

        stat._99Percentile = vect.at((int) std::round(vect.size() * 0.99) - 1);

    }

    void Experiment::calculateVariation_StandardDeviation(Statistics &stat, std::vector<double> vect) {

        stat.variance = 0;

        for (int i = 0; i < vect.size(); i++) {
            stat.variance += std::pow(stat.average - vect[i], 2);
        }

        stat.variance = stat.variance / vect.size();
        stat.standardDeviation = std::sqrt(stat.variance);

    }


    void Experiment::calculatePerformanceMeasures(double duration) {

        SLOG("Calculating performance measurements!");

        // calculate perf numbers
        double totalOps = config.TotalNumOps;
        double totalTime = duration;
        double opRate = totalOps / totalTime;
        double totalReadLatency = 0.0;
        double totalWriteLatency = 0.0;
        double totalTxLatency = 0.0;

        double totalRead = 0.0;
        double totalWrite = 0.0;

        double minReadLatency = threadArgs[0]->MinReadLatency;
        double minWriteLatency = threadArgs[0]->MinWriteLatency;
        double maxReadLatency = threadArgs[0]->MaxReadLatency;
        double maxWriteLatency = threadArgs[0]->MaxWriteLatency;

        Results results;

        results.OpGetLatencyStats.average = 0;
        results.OpGetLatencyStats.max = 0;
        results.OpGetLatencyStats.min = 100000000;
        results.NumHotBlockedKeys = 0;

        results.OpSetLatencyStats.average = 0;
        results.OpSetLatencyStats.max = 0;
        results.OpSetLatencyStats.min = 100000000;

        for (int i = 0; i < config.NumThreads; i++) {

//#ifdef MEASURE_STATISTICS
            calculateLatencyMeasurements(threadArgs[i]->OpReadLatencies, results.readLatencies,
                                         totalReadLatency,
                                         minReadLatency, maxReadLatency);
            calculateLatencyMeasurements(threadArgs[i]->OpWriteLatencies, results.writeLatencies,
                                         totalWriteLatency,
                                         minWriteLatency, maxWriteLatency);
//#endif

            results.OpGetLatencyStats.average += (threadArgs[i]->ReadLatenciesSum);
            totalRead += threadArgs[i]->TotalNumReadOps;
            results.OpGetLatencyStats.max = max(results.OpGetLatencyStats.max, threadArgs[i]->MaxReadLatency);
            results.OpGetLatencyStats.min = min(results.OpGetLatencyStats.min, threadArgs[i]->MinReadLatency);

            results.OpSetLatencyStats.average += (threadArgs[i]->WriteLatenciesSum);
            totalWrite += threadArgs[i]->TotalNumWriteOps;
            results.OpSetLatencyStats.max = max(results.OpSetLatencyStats.max, threadArgs[i]->MaxWriteLatency);
            results.OpSetLatencyStats.min = min(results.OpSetLatencyStats.min, threadArgs[i]->MinWriteLatency);

            results.NumHotBlockedKeys += threadArgs[i]->numHotBlockKeys;
        }

        results.OpGetLatencyStats.average /= totalRead;
        results.OpSetLatencyStats.average /= totalWrite;
        results.TotalReadOps = totalRead;
        results.TotalWriteOps = totalWrite;
        results.NumOpsPerThread = config.NumOpsPerThread;
        results.NumPartitions = config.NumPartitions;
        results.NumReplicas = config.NumReplicasPerPartition;
        results.OpThroughput = opRate;
        results.NumThreadsPerReplica = config.NumThreads;
        results.TotalOps = totalOps;
        results.TotalTime = totalTime;

//#ifdef MEASURE_STATISTICS
        calculateMedian_Percentiles(results.readLatencies, results.OpGetLatencyStats);
        calculateMedian_Percentiles(results.writeLatencies, results.OpSetLatencyStats);
        calculateVariation_StandardDeviation(results.OpGetLatencyStats, results.readLatencies);
        calculateVariation_StandardDeviation(results.OpSetLatencyStats, results.writeLatencies);
//#endif

        writeResults(stdout, results);
    }

    void Experiment::freeResources() {

        for (int i = 0; i < config.NumThreads; i++) {
            delete threadArgs[i];
            delete threads[i];
        }
    }

    void Experiment::runExperiment() {

        buildThreadArguments();

        // launch benchmarking client threads
        launchBenchmarkingClientThreads();
        // wait for all threads ready to run
        WaitHandle::WaitAll(opReadyEvents);

        // record start time
        PhysicalTimeSpec startTimeExp = Utils::GetCurrentClockTime();

        // signal threads to start
        for (int i = 0; i < config.NumThreads; i++) {
            threadArgs[i]->OpStartEvent.Set();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(this->config.experimentDuration));
        this->stopOperation = true;
        fprintf(stdout, "Stopping the experiment. Waiting for the clients to finish.\n");
        fflush(stdout);
        // wait for all threads finishing
        WaitHandle::WaitAll(opEndEvents);

        // record end time
        PhysicalTimeSpec endTimeExp = Utils::GetCurrentClockTime();
        double duration = (endTimeExp - startTimeExp).toMilliSeconds();
        fprintf(stdout, "[INFO] ALL CLIENT OPERATIONS ARE DONE\n");
        fflush(stdout);

        config.TotalNumOps = 0;
        config.NumOpsPerThread = 0;

        SLOG("Calculating performance statistics");
        for (int i = 0; i < config.NumThreads; i++) {
            config.TotalNumOps += threadArgs[i]->NumOpsPerThread;
            config.NumOpsPerThread += threadArgs[i]->NumOpsPerThread;
        }
        config.NumOpsPerThread /= config.NumThreads;

        calculatePerformanceMeasures(duration);

        // free resources
        freeResources();
        SLOG("Resources are FREE!");

    }

}
