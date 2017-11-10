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


#include "groupservice/group_server.h"
#include <stdlib.h>

using namespace scc;

int main(int argc, char* argv[]) {
  if (argc != 4) {
    fprintf(stdout,"Number of args: %d ", argc);

    for (int i = 0; i< argc; i++){
      fprintf(stdout," %s ", argv[i]);
    }
    fprintf(stdout, "Usage: %s <Port> <NumPartitions> <NumReplicasPerPartition>\n", argv[0]);
    exit(1);
  }

  int port = atoi(argv[1]);
  int numPartitions = atoi(argv[2]);
  int numReplicaPerPartition = atoi(argv[3]);


  fprintf(stdout,"GroupServer program... \n");
  GroupServer server(port, numPartitions, numReplicaPerPartition);
  fprintf(stdout,"GroupServer connected. \n");
  server.Run();
}
