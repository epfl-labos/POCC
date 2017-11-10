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


#include "kvservice/public_kv_client.h"
#include "common/exceptions.h"
#include "common/sys_config.h"
#include "common/utils.h"
#include <string>
#include <stdlib.h>
#include <iostream>
#include <boost/algorithm/string.hpp>

using namespace scc;

void showUsage();

int main(int argc, char* argv[]) {

    if (argc != 4 && argc != 5) {
        fprintf(stdout, "Usage: %s <Causal> <ServerName> <ServerPort>\n", argv[0]);
        fprintf(stdout, "Usage: %s <Causal> <ServerName> <ServerPort> <command>\n", argv[0]);
        fprintf(stdout, "Found %d args\n", argc);
        exit(1);
    }

    SysConfig::Consistency = Utils::str2consistency(argv[1]);
    std::string serverName = argv[2];
    unsigned short serverPort = atoi(argv[3]);
    std::string cmd;
    if (argc == 5) {
        cmd = argv[4];
    }

    try {
        // connect to the server
        PublicKVClient client(serverName, serverPort);
        do {
            std::string input;

            if (argc == 5) {
                input = argv[4];
            } else {
                std::cout << ">"; // wait for user input
                std::getline(std::cin, input);
            }

            std::vector<string> splitedInput;
            boost::split(splitedInput, input, boost::is_any_of(" "));
            bool inputInvalid = true;

            if (splitedInput[0] == "Echo") {
                if (splitedInput.size() == 2) {
                    std::string input = splitedInput[1];
                    std::string output;
                    client.Echo(input, output);
                    cout << output << "\n";
                    inputInvalid = false;
                }
            } else if (splitedInput[0] == "Get") {
                if (splitedInput.size() == 2) {
                    std::string key = splitedInput[1];
                    std::string getValue;
                    bool r = client.Get(key, getValue);
                    if (r) {
                        cout << "Got " << key << " = " << getValue << "\n";
                    } else {
                        std::cout << "Operation failed.\n";
                    }
                    inputInvalid = false;
                }
            } else if (splitedInput[0] == "Check") {
                if (splitedInput.size() == 3) {
                    std::string key = splitedInput[1];
                    std::string expectedValue = splitedInput[2];
                    std::string getValue;
                    bool r = client.Get(key, getValue);
                    if (r) {
                        if (expectedValue == getValue) {
                            cout << "[OK] " << key << " == " << getValue << endl;
                        } else {
                            cout << "ERROR! Expected [" << expectedValue << "] but found [" << getValue << "]" << endl;
                        }
                    } else {
                        cout << "Operation failed.\n";
                    }
                    inputInvalid = false;
                }
            } else if (splitedInput[0] == "Set") {
                if (splitedInput.size() == 3) {
                    std::string key = splitedInput[1];
                    std::string value = splitedInput[2];
                    cout << value << endl;
                    bool r = client.Set(key, value);
                    if (r) {
                        std::cout << "Assigned " << key << " = " << value << endl;
                    } else {
                        cout << "Operation failed.\n";
                    }
                    inputInvalid = false;
                }
            } else if (splitedInput[0] == "ShowItem") {
                if (splitedInput.size() == 2) {
                    std::string key = splitedInput[1];
                    std::string itemVersions;
                    bool r = client.ShowItem(key, itemVersions);
                    if (r) {
                        cout << itemVersions << "\n";
                    } else {
                        cout << "Operation failed.\n";
                    }
                    inputInvalid = false;
                }
            } else if (splitedInput[0] == "ShowDB") {
                if (splitedInput.size() == 1) {
                    std::string allItemVersions;
                    bool r = client.ShowDB(allItemVersions);
                    if (r) {
                        cout << allItemVersions << "\n";
                    } else {
                        cout << "Operation failed.\n";
                    }
                    inputInvalid = false;
                }
            } else if (splitedInput[0] == "ShowState") {
                if (splitedInput.size() == 1) {
                    std::string stateStr;
                    bool r = client.ShowState(stateStr);
                    if (r) {
                        cout << stateStr << "\n";
                    } else {
                        cout << "Operation failed.\n";
                    }
                    inputInvalid = false;
                }
            } else if (splitedInput[0] == "DumpLatency") {
                if (splitedInput.size() == 1) {
                    std::string resultStr;
                    bool r = client.DumpLatencyMeasurement(resultStr);
                    if (r) {
                        cout << resultStr << "\n";
                    } else {
                        cout << "Operation failed.\n";
                    }
                    inputInvalid = false;
                }
            } else if (splitedInput[0] == "PartitionId") {
                if (splitedInput.size() == 3) {
                    std::string key = splitedInput[1];
                    int numPartitions = atoi(splitedInput[2].c_str());
                    std::cout << Utils::strhash(key) % numPartitions << "\n";
                    inputInvalid = false;
                }
            } else if (splitedInput[0] == "Test") {
                if (splitedInput.size() == 3) {
                    std::string key = splitedInput[1];
                    std::string expectedValue = splitedInput[2];
                    std::string getValue;
                    bool r = client.Get(key, getValue);
                    if (r) {
                        if (expectedValue == getValue) {
                            std::cout << "[OK] " << key << " == " << getValue << endl;
                        } else {
                            std::cout << "ERROR! Expected [" << expectedValue << "] but found [" << getValue << "]" << endl;
                        }
                    } else {
                        cout << "Operation failed.\n";
                    }
                    inputInvalid = false;
                }
            }

            if (inputInvalid) {
                std::cout << "Invalid input.\n";
                showUsage();
            }
        } while (argc == 4);

    } catch (SocketException& e) {
        fprintf(stdout, "SocketException: %s\n", e.what());
        exit(1);
    }

}

void showUsage() {
    std::string usage = "Echo <text>\n"
            "Get <key>\n"
            "Set <key> <value>\n"
            "ShowItem <key>\n"
            "ShowDB\n"
            "ShowState\n"
            "DumpLatency\n"
            "PartitionId <key> <number of partitions>\n";
    fprintf(stdout, "%s", usage.c_str());
}
