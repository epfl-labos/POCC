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


#ifndef SCC_COMMON_SYS_STATS_H
#define SCC_COMMON_SYS_STATS_H

#include <atomic>

namespace scc {

    class SysStats {
    public:
        static std::atomic<int> NumPublicGetRequests;
        static std::atomic<int> NumPublicSetRequests;
        static std::atomic<int> NumInternalGetRequests;
        static std::atomic<int> NumInternalSetRequests;

        static std::atomic<int> NumDelayedLocalUpdates;
        static std::atomic<long> SumDelayedLocalUpdates;

        static std::atomic<int> NumPendingPropagatedUpdates;
        static std::atomic<int> NumRecvUpdateReplicationMsgs;
        static std::atomic<int> NumRecvUpdateReplications;
        static std::atomic<int> NumReceivedPropagatedUpdates;
        static std::atomic<int> NumSentUpdates;
        static std::atomic<int> NumSentBatches;
        static std::atomic<int> NumReceivedBatches;
        static std::atomic<int> NumRecvHeartbeats;
        static std::atomic<int> NumUpdatesStoredInLocalUpdateQueue;
        static std::atomic<int> NumUpdatesStoredInToPropagateLocalUpdateQueue;
        static std::atomic<int> NumUPropagatedPersistedUpdates;
        static std::atomic<int> NumGSTRounds;


        static std::atomic<int> NumRecvGSTs;
        static std::atomic<int> NumInitiatedGSTs;
        static std::atomic<int> NumBroadcastedLSTs;
        static std::atomic<int> NumRecvGSTBytes;
        static std::atomic<int> NumSendInternalLSTBytes;
        static std::atomic<int> NumSentGSTBytes;
        static std::atomic<int> NumRecvInternalLSTBytes;


        static std::atomic<int> NumSentUSVBytes;
        static std::atomic<int> NumRecvUSVBytes;
        static std::atomic<int> NumSentReplicationBytes;
        static std::atomic<int> NumRecvReplicationBytes;


        static std::atomic<int> NumReturnedItemVersions;
        static std::atomic<int> NumReturnedStaleItemVersions;
        static std::atomic<int> NumReturnedLocalItemVersions;
        static std::atomic<int> NumReturnedImmediatelyLocalItemVersions;
        static std::atomic<int> NumReturnedRemoteItemVersions;
        static std::atomic<int> NumReturnedLatestAddedItemVersions;
        static std::atomic<int> NumReturnedNOTLatestAddedItemVersions;
        static std::mutex StalenessTimeMutex;
        static std::vector<double> StalenessTime;
        static std::mutex NumFresherVersionsInItemChainMutex;
        static std::vector<int64_t> NumFresherVersionsInItemChain;
        static std::mutex UserPerceivedStalenessTimeMutex;
        static std::vector<double> UserPerceivedStalenessTime;
        static std::atomic<double> MinUserPerceivedStalenessTime;
        static std::atomic<double> MaxUserPerceivedStalenessTime;

        static std::atomic<int>NumTimesReturnedUnmergedItemVersion;
        static std::mutex NumUnmergedVersionsInItemChainMutex;
        static std::vector<int64_t> NumUnmergedVersionsInItemChain;


        static int NumBlocks;
        static int NumBlocksCond1;
        static int NumBlocksCond2;
        static int NumUselessBlocks;
        static std::mutex BlockDurationMutex;
        static std::vector<double> BlockDuration;
        static int NumLSTCatchup;

        static std::atomic<int> NumXactBlocks;
        static std::atomic<long> XactBlockDuration;
        static std::atomic<int> NumXacts;
    };

}

#endif
