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


#include "common/sys_config.h"

namespace scc {

    ConsistencyType SysConfig::Consistency = ConsistencyType::Causal;
    bool SysConfig::OptimisticMode = false;
    GSTDerivationType SysConfig::GSTDerivationMode = GSTDerivationType::TREE; 
    int SysConfig::NumItemIndexTables = 8;
    std::string SysConfig::PreallocatedOpLogFile = "/data/txlog.bin";
    std::string SysConfig::OpLogFilePrefix = "/data/txlog.bin.";
    DurabilityType SysConfig::DurabilityOption = DurabilityType::Memory;
    std::string SysConfig::SysLogFilePrefix = "/tmp/syslog.txt.";
    std::string SysConfig::SysDebugFilePrefix = "/root/grain/tools/debug/sysdebug_";
    bool SysConfig::SysLogEchoToConsole = true;
    bool SysConfig::SysDebugEchoToConsole = false;
    bool SysConfig::Debugging = false;
    int SysConfig::NumChannelsPartition = 32;
    int SysConfig::UpdatePropagationBatchTime = 5000; // microseconds

    int SysConfig::ReplicationHeartbeatInterval = 2000; // microseconds

    int SysConfig::NumChildrenOfTreeNode = 2;
    int SysConfig::GSTComputationInterval = 5000; // microseconds

    bool SysConfig::MeasureVisibilityLatency = false;
    int SysConfig::LatencySampleInterval = 1; // number of operations
    bool SysConfig::WaitConditionInOptimisticSetOperation = true;

    int SysConfig::GetSpinTime = 500;
    int SysConfig::OpttxDelta = 0;

} // namespace scc
