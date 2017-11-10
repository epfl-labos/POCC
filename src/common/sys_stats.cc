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


#include <vector>
#include <mutex>
#include "common/sys_stats.h"

namespace scc {
    std::atomic<int> SysStats::NumPublicGetRequests(0);
    std::atomic<int> SysStats::NumPublicSetRequests(0);
    std::atomic<int> SysStats::NumInternalGetRequests(0);
    std::atomic<int> SysStats::NumInternalSetRequests(0);

    std::atomic<int> SysStats::NumDelayedLocalUpdates(0);
    std::atomic<long> SysStats::SumDelayedLocalUpdates(0);

    std::atomic<int> SysStats::NumPendingPropagatedUpdates(0);
    std::atomic<int> SysStats::NumRecvUpdateReplicationMsgs(0);
    std::atomic<int> SysStats::NumRecvUpdateReplications(0);
    std::atomic<int> SysStats::NumReceivedPropagatedUpdates(0);
    std::atomic<int> SysStats::NumSentUpdates(0);
    std::atomic<int> SysStats::NumSentBatches(0);
    std::atomic<int> SysStats::NumReceivedBatches(0);
    std::atomic<int> SysStats::NumRecvHeartbeats(0);
    std::atomic<int> SysStats::NumUpdatesStoredInLocalUpdateQueue(0);
    std::atomic<int> SysStats::NumUpdatesStoredInToPropagateLocalUpdateQueue(0);
    std::atomic<int> SysStats::NumUPropagatedPersistedUpdates(0);
    std::atomic<int> SysStats::NumGSTRounds(0);

    std::atomic<int> SysStats::NumRecvGSTs(0);
    std::atomic<int> SysStats::NumInitiatedGSTs(0);
    std::atomic<int> SysStats::NumBroadcastedLSTs(0);
    std::atomic<int> SysStats::NumRecvGSTBytes(0);
    std::atomic<int> SysStats::NumSentGSTBytes(0);
    std::atomic<int> SysStats::NumSendInternalLSTBytes(0);
    std::atomic<int> SysStats::NumRecvInternalLSTBytes(0);

    std::atomic<int> SysStats::NumSentUSVBytes(0);
    std::atomic<int> SysStats::NumRecvUSVBytes(0);
    std::atomic<int> SysStats::NumSentReplicationBytes(0);
    std::atomic<int> SysStats::NumRecvReplicationBytes(0);

    std::atomic<int> SysStats::NumReturnedItemVersions(0);
    std::atomic<int> SysStats::NumReturnedStaleItemVersions(0);
    std::atomic<int> SysStats::NumReturnedLocalItemVersions(0);
    std::atomic<int> SysStats::NumReturnedImmediatelyLocalItemVersions(0);
    std::atomic<int> SysStats::NumReturnedRemoteItemVersions(0);
    std::atomic<int> SysStats::NumReturnedLatestAddedItemVersions(0);
    std::atomic<int> SysStats::NumReturnedNOTLatestAddedItemVersions(0);
    std::mutex SysStats::StalenessTimeMutex;
    std::vector<double> SysStats::StalenessTime;
    std::mutex SysStats::NumFresherVersionsInItemChainMutex;
    std::vector<int64_t> SysStats::NumFresherVersionsInItemChain(0);
    std::mutex SysStats::UserPerceivedStalenessTimeMutex;
    std::vector<double> SysStats::UserPerceivedStalenessTime(0.0);
    std::atomic<double> SysStats::MinUserPerceivedStalenessTime(1000000.0);
    std::atomic<double> SysStats::MaxUserPerceivedStalenessTime(0.0);

    std::atomic<int> SysStats::NumTimesReturnedUnmergedItemVersion(0);
    std::mutex SysStats::NumUnmergedVersionsInItemChainMutex;
    std::vector<int64_t> SysStats::NumUnmergedVersionsInItemChain;

    int SysStats::NumBlocks(0);
    int SysStats::NumBlocksCond1(0);
    int SysStats::NumBlocksCond2(0);
    int SysStats::NumLSTCatchup(0);
    int SysStats::NumUselessBlocks(0);
    std::mutex SysStats::BlockDurationMutex;
    std::vector<double> SysStats::BlockDuration(0.0);

    std::atomic<int> SysStats::NumXactBlocks(0);
    std::atomic<long> SysStats::XactBlockDuration(0.0);
    std::atomic<int> SysStats::NumXacts(0);

} // namespace scc
