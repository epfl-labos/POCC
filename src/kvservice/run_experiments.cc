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
#include "kvservice/experiment.h"
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

using namespace scc;

void showUsage(char *program) {
    fprintf(stdout,
            "Usage: %s <Causal> <RequestDistribution> <RequestDistributionParameter> <ReadRatio> <WriteRatio> <ManagerName> <ManagerPort> <TotalNumItems> <WriteValueSize> <NumOpsPerThread> <NumThreads> <ServingReplicaId> <ClientServerId> <LatencyOutputFileName> <ReservoirSampling> <ReservoirSamplingLimit> <ExperimentDuration[ms]> <ClientSessionResetLimit> <EnableClientSessionResetLimit> <NumTxReadItems> <numTxReadItems>\n",
            program);
}

int main(int argc, char *argv[]) {
    try {
        if (argc != 25) {
            fprintf(stdout, "argc=%d\n", argc);
            showUsage(argv[0]);
            exit(1);
        }

        SysConfig::Consistency = Utils::str2consistency(argv[1]);

        Experiment exp(argv);

        try {
            exp.runExperiment();
        } catch (SocketException &e) {
            fprintf(stdout, "SocketException: %s\n", e.what());
            exit(1);
        } catch (...) {
            fprintf(stdout, "Exception occurred during running the experiment\n");
            exit(1);
        }

    } catch (...) {
        fprintf(stdout, "Exception occurred during running the experiment\n");
        exit(1);
    }
}
