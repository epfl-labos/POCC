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


#ifndef SCC_KVSERVICE_EXPERIMENT_H
#define SCC_KVSERVICE_EXPERIMENT_H

#include "common/types.h"
#include "common/generators.h"
#include "common/sys_config.h"
#include "kvservice/public_kv_client_lb.h"
#include "kvservice/public_kv_client.h"
#include "groupservice/group_client.h"
#include "common/utils.h"
#include "common/types.h"
#include "common/wait_handle.h"
#include "common/sys_config.h"
#include "common/generators.h"
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <string>
#include <numeric>
#include <time.h>
#include <math.h>
#include <thread>
#include <chrono>
#include <algorithm>
#include <iostream>
#include <stdexcept>


namespace scc {

    typedef struct {
        int ThreadId;
        std::string workload;
        std::vector<std::string> ServerNames;
        std::vector<int> ServerPorts;
        std::vector<PublicClientLB *> Clients;
        PublicClientLB *Client;
        int LocalPartitionId;
        int NumPartitions;
        int NumReplicas;
        int TotalNumItems;
        int NumOpsPerThread;
        int NumValueBytes;
        WaitHandle OpReadyEvent;
        WaitHandle OpStartEvent;
        WaitHandle OpEndEvent;
        std::vector<double> OpLatencies;
        std::vector<double> OpReadLatencies;
        std::vector<double> OpWriteLatencies;
        int ReadRatio;
        int WriteRatio;
        bool ReservoirSampling;
        int ReservoirSamplingLimit;
        atomic<bool> *stopOperation;
        int servingReplica;
        int servingPartition;
        double ReadLatenciesSum;
        int TotalNumReadOps;
        double MinReadLatency;
        double MaxReadLatency;
        double WriteLatenciesSum;
        int TotalNumWriteOps;
        double MinWriteLatency;
        double MaxWriteLatency;
        int numHotBlockKeys;
        int clientResetNumber;
        bool enableClientReset;
        int NumTxReadItems;
        std::string RequestsDistribution;
        Generator<uint64_t> *generator;
        int locality;
        int thinkTime;

    } ThreadArg;

    typedef struct {
        int NumPartitions;
        int NumReplicas;
        int NumThreadsPerReplica;
        int NumOpsPerThread;
        int NumHotBlockedKeys;
        double TotalOps;
        double TotalReadOps;
        double TotalWriteOps;
        double TotalTime;
        double OpThroughput;
        Statistics OpGetLatencyStats;
        Statistics OpSetLatencyStats;
        std::vector<double> readLatencies;
        std::vector<double> writeLatencies;


    } Results;

    class Experiment {
    public:
        std::atomic<bool> stopOperation;

        Experiment(char *argv[]);

        void runExperiment();

        Configuration config;
    private:
        std::vector<std::vector<DBPartition>> allPartitions;
        std::vector<ThreadArg *> threadArgs;
        std::vector<std::thread *> threads;
        std::list<WaitHandle *> opReadyEvents;
        std::list<WaitHandle *> opEndEvents;

        std::string workloadType;

        void initializeThreadRequestDistribution(ThreadArg *t);

        void buildThreadArguments();

        void launchBenchmarkingClientThreads();

        void calculatePerformanceMeasures(double duration);

        void writeResults(FILE *&stream, Results res);

        void freeResources();

        void printExperimentParameters();

        void writeLatencyDistribution(std::string fileName, vector<double> vector);

        void calculateVariation_StandardDeviation(Statistics &stat, vector<double> vect);

        void calculateMedian_Percentiles(vector<double> vect, Statistics &stat);
    };

} // namespace scc

#endif
