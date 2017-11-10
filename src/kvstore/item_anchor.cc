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


#include "kvstore/item_anchor.h"
#include "kvstore/mv_kvstore.h"
#include "common/sys_config.h"
#include "common/sys_stats.h"
#include "common/utils.h"
#include <string>
#include <boost/format.hpp>
#include <iostream>
#include "common/sys_logger.h"

#ifdef ATOMIC_LATEST_VERSION
#define LATEST_VERSION(A) {\
                    std::lock_guard<std::mutex> lk(_itemMutex);\
                    A = _latestVersion;\
                        }
#else
#define LATEST_VERSION(A) for(int i=0;i<_latestVersion.size();i++){\
                                A.push_back(_latestVersion[i]);\
                        }
#endif

namespace scc {
    int ItemAnchor::_replicaId(0);

    ItemAnchor::ItemAnchor(std::string itemKey, int numReplicas) {
        _latestVersion.resize(numReplicas);

        _itemKey = itemKey;
    }

    ItemAnchor::~ItemAnchor() {
    }

    void ItemAnchor::InsertVersion(ItemVersion *version) {
        std::lock_guard<std::mutex> lk(_itemMutex);

        ASSERT(version != NULL);
        int replica = version->SrcReplica;
        ItemVersion *lv = _latestVersion.at(replica);

        // head is NULL
        if (lv == NULL) {
            version->Next = NULL;
            _latestVersion[replica] = version;
            _lastAddedVersion = version;
            ASSERT(_latestVersion[replica] != NULL);
            return;
        }

        ASSERT(*version > *lv);

        // insert to head
        version->Next = lv;
        _latestVersion[replica] = version;
        if (_lastAddedVersion->PUT < version->PUT)
            _lastAddedVersion = version;
        ASSERT(_latestVersion[replica] != NULL);
    }

#ifdef DEP_VECTORS
    ItemVersion *ItemAnchor::LatestCausalVersion(int localReplicaId, ConsistencyMetadata &cdata) {
        if (SysConfig::OptimisticMode)
            return LatestCausalVersionOpt(localReplicaId, cdata);
        return LatestCausalVersionPess2(localReplicaId, cdata);
    }

#else
    ItemVersion *ItemAnchor::LatestCausalVersion(int localReplicaId, PhysicalTimeSpec &t) {
        if (SysConfig::OptimisticMode)
            return LatestCausalVersionOpt(localReplicaId, t);
        return LatestCausalVersionPess(localReplicaId, t);
    }

#endif

#ifdef DEP_VECTORS
    ItemVersion *ItemAnchor::LatestCausalVersionOpt(int localReplicaId, ConsistencyMetadata &cdata) {
        bool block1 = false;
#ifdef MEASURE_STATISTICS
        PhysicalTimeSpec startTimeOfBlock = Utils::GetCurrentClockTime();
#endif

#ifndef FALSE_BLOCK
        block1 = (MVKVStore::Instance()->waitOnReadOnly(cdata));
#endif

#ifdef MEASURE_STATISTICS
        SysStats::NumReturnedItemVersions += 1;
        if (block1) {
            // record end time
            PhysicalTimeSpec endTimeOfBlock = Utils::GetCurrentClockTime();
            double duration = (endTimeOfBlock - startTimeOfBlock).toMilliSeconds();
            std::lock_guard<std::mutex> lk(SysStats::BlockDurationMutex);
            SysStats::BlockDuration.push_back(duration);

            SysStats::NumBlocks += 1;
            SysStats::NumBlocksCond1 += 1;

        }

#endif

        return _lastAddedVersion;
    }


#else // SCALAR

    ItemVersion *ItemAnchor::LatestCausalVersionOpt(int localReplicaId, PhysicalTimeSpec &minLST) {
        ItemVersion *next = NULL;

#ifdef MEASURE_STATISTICS
        bool block1 = false;
        bool block2 = false;
        bool lst_catches_up = true;

        PhysicalTimeSpec startTimeOfBlock = Utils::GetCurrentClockTime();
#endif
        PhysicalTimeSpec lst;

        MVKVStore *store_ptr = MVKVStore::Instance();

        while (minLST > (lst = store_ptr->GetLST())) {
#ifdef MEASURE_STATISTICS
            block1 = true;
#endif

            next = _lastAddedVersion;

            ASSERT(next != NULL);

            if (next->PUT > minLST) {
#ifdef MEASURE_STATISTICS
                lst_catches_up = false; //The unblocking has been because of a fresher key and not because of the lst catching up
#endif
                break;
            }

#ifdef MEASURE_STATISTICS
            block2 = true;
#endif
            std::this_thread::sleep_for(std::chrono::microseconds(SysConfig::GetSpinTime));

            next = NULL;
        }

#ifdef MEASURE_STATISTICS
        if (block1) {
            std::lock_guard <std::mutex> lk(SysStats::BlockDurationMutex);

            // record end time
            PhysicalTimeSpec endTimeOfBlock = Utils::GetCurrentClockTime();
            double duration = (endTimeOfBlock - startTimeOfBlock).toMilliSeconds();

            SysStats::BlockDuration.push_back(duration);

            SysStats::NumBlocks += 1;

            SysStats::NumBlocksCond1 += 1;

            if (lst_catches_up) {
                //Increase this counter only if you have been blocked because of the lst in the first place
                SysStats::NumLSTCatchup += 1;
            }

            if (block2) {
                SysStats::NumBlocksCond2 += 1;
            }

        }
#endif

        if (next == NULL) {
            next = _lastAddedVersion;
        }

        ASSERT(next != NULL);

#ifdef MEASURE_STATISTICS
        if (lst_catches_up && block1) {
            PhysicalTimeSpec localCreationTime;
            if (next->SrcReplica == localReplicaId) {
                localCreationTime = next->PUT;
            }
            else {

                localCreationTime = next->RIT;
            }
            if (localCreationTime < startTimeOfBlock) {
                std::lock_guard <std::mutex> lk(SysStats::BlockDurationMutex);
                SysStats::NumUselessBlocks += 1;
            }
        }
#endif

#ifdef MEASURE_STATISTICS
        SysStats::NumReturnedItemVersions += 1;
#endif

        return next;
    }

    //PESSIMISTIC
    ItemVersion *ItemAnchor::LatestCausalVersionPess(int localReplicaId, PhysicalTimeSpec &gst) {
        ItemVersion *next = NULL;
        ASSERT(!gst.Zero());
        //PESSIMISTIC MODE

#ifdef MEASURE_STATISTICS
        std::string stalenessStr = "";
        PhysicalTimeSpec timeGet = Utils::GetCurrentClockTime();
        ItemVersion *firstNotVisibleItem = NULL;
        bool stale = false;
        int numVersionsBehind = 0;
#endif

        next = _lastAddedVersion;
        ASSERT(next != NULL);
        //If the lastAddedVersion is not visibile, then scan the chain.
        if (!(next->SrcReplica == localReplicaId || next->PUT <= gst)) {

            std::vector<ItemVersion *> current;
            LATEST_VERSION(current);

            while (true) {

                int nextIndex = 0;
                next = current[0];

                for (int i = 1; i < current.size(); ++i) {
                    ItemVersion *cur = current[i];

                    if (next == NULL || (cur != NULL && next < cur)) {
                        next = cur;
                        nextIndex = i;
                    }
                }
                ASSERT(next != NULL);
                if (next->SrcReplica == localReplicaId || next->PUT <= gst) {
                    break;
                }

#ifdef MEASURE_STATISTICS
                    stale = true;
                    firstNotVisibleItem = next;
                    numVersionsBehind++;
#endif
                ASSERT(current[nextIndex] == next);
                current[nextIndex] = next->Next;
            }
            ASSERT(next != NULL);
        }

#ifdef MEASURE_STATISTICS
        if (stale) {
            SysStats::NumReturnedStaleItemVersions++;

            ASSERT(firstNotVisibleItem != NULL);
            _calculateUserPerceivedStalenessTime(firstNotVisibleItem, -1, timeGet,
                                                 stalenessStr);

            {
                std::lock_guard<std::mutex> lk(SysStats::NumFresherVersionsInItemChainMutex);
                SysStats::NumFresherVersionsInItemChain.
                        push_back(numVersionsBehind);
            }
        }
#endif

#ifdef MEASURE_STATISTICS
        SysStats::NumReturnedItemVersions += 1;
#endif

        return next;
    }

#endif

    void ItemAnchor::_calculateUserPerceivedStalenessTime(ItemVersion *firstNotVisibleItem, int replicaId,
                                                          PhysicalTimeSpec timeGet, std::string stalenessStr) {
        PhysicalTimeSpec timeNow = Utils::GetCurrentClockTime();

        double resTime = (timeNow - firstNotVisibleItem->RIT).toMilliSeconds();

        ASSERT(resTime >= 0);
        ASSERT(resTime < 3600000);

        {
            std::lock_guard<std::mutex> lk(SysStats::UserPerceivedStalenessTimeMutex);
            SysStats::UserPerceivedStalenessTime.push_back(resTime);
        }

        if (resTime > SysStats::MaxUserPerceivedStalenessTime) {
            SysStats::MaxUserPerceivedStalenessTime = resTime;
        }

        if (resTime < SysStats::MinUserPerceivedStalenessTime) {
            SysStats::MinUserPerceivedStalenessTime = resTime;
        }
    }


#ifndef DEP_VECTORS //SCALAR

    ItemVersion *ItemAnchor::LatestSnapshotVersion(PhysicalTimeSpec st) {
        std::vector<ItemVersion *> current;
        LATEST_VERSION(current);
        ItemVersion *next = NULL;

#ifdef MEASURE_STATISTICS
        std::string stalenessStr = "";
        PhysicalTimeSpec timeGet = Utils::GetCurrentClockTime();
        ItemVersion *firstNotVisibleItem = NULL;
        bool stale = false;
        int numVersionsBehind = 0;
#endif

        while (true) {

            int nextIndex = 0;
            next = current[0];

            for (int i = 1; i < current.size(); ++i) {
                ItemVersion *cur = current[i];

                if (next == NULL || (cur != NULL && next < cur)) {
                    next = cur;
                    nextIndex = i;
                }
            }

            ASSERT(next != NULL);

            if (next->PUT <= st) {
                break;
            }

#ifdef MEASURE_STATISTICS
                stale = true;
                firstNotVisibleItem = next;
                numVersionsBehind++;
#endif

            ASSERT(current[nextIndex] == next);
            current[nextIndex] = next->Next;

        }
        ASSERT(next != NULL);
#ifdef MEASURE_STATISTICS
        if (stale) {
                    SysStats::NumReturnedStaleItemVersions++;

                    ASSERT(firstNotVisibleItem != NULL);
                    _calculateUserPerceivedStalenessTime(firstNotVisibleItem,-1, timeGet,
                                                         stalenessStr);

                    {
                        std::lock_guard<std::mutex> lk(SysStats::NumFresherVersionsInItemChainMutex);
                        SysStats::NumFresherVersionsInItemChain.
                                push_back(numVersionsBehind);
                    }
                }

        SysStats::NumReturnedItemVersions += 1;
#endif
        return next;
    }

#else //VECTOR

    ItemVersion *ItemAnchor::LatestSnapshotVersion(std::vector<PhysicalTimeSpec> &st, int localReplicaId) {
#ifdef MEASURE_STATISTICS
        std::string stalenessStr = "";
        PhysicalTimeSpec timeGet = Utils::GetCurrentClockTime();
        ItemVersion *firstNotVisibleItem = NULL;
        bool stale = false;
        int numVersionsBehind = 0;
        int numUnmergedVersions = 0;
#endif
        std::vector<ItemVersion *> current;
        LATEST_VERSION(current);
        ItemVersion *next = NULL;
        std::vector<PhysicalTimeSpec> txTime = st;
        int j;
        while (true) {

            int nextIndex = 0;
            next = current[0];

            for (int i = 1; i < current.size(); ++i) {
                ItemVersion *cur = current[i];

                if (next == NULL || (cur != NULL && next < cur)) {
                    next = cur;
                    nextIndex = i;
                }
            }

            ASSERT(next != NULL);

            bool visible = true;

            for (j = 0; j < txTime.size(); j++) {
                if (next->DV[j] > txTime[j]) {
                    visible = false;
                    break;
                }
            }

            if (visible) { break; }

#ifdef MEASURE_STATISTICS
            stale = true;
            firstNotVisibleItem = next;
            numVersionsBehind++;
            numUnmergedVersions++;
#endif

            ASSERT(current[nextIndex] == next);
            current[nextIndex] = next->Next;
        }
        ASSERT(next != NULL);

#ifdef MEASURE_STATISTICS
        //Search for unmerged items for every replica
        for (int i = 0; i < current.size(); i++) {
            if (i == localReplicaId) {
                continue;
            }

            ItemVersion *chainItem = current[i];

            while (true) {
                if (chainItem == NULL) {
                    //The end of the chain is reached
                    break;
                } else {
                    //Check visibility of the item
                    bool isVisible = true;
                    for (int j = 0; j < chainItem->DV.size(); j++) {
                        if (j == localReplicaId) { //Items from local replica are always visible
                            continue;
                        }

                        if (chainItem->DV[j] > txTime[j]) {
                            isVisible = false;
                            numUnmergedVersions++;
                            break;
                        }
                    }

                    if (isVisible) {
                        break;
                    } else {
                        chainItem = chainItem->Next;
                    }
                }
            }
        }

        if (stale) {
            SysStats::NumReturnedStaleItemVersions++;

            ASSERT(firstNotVisibleItem != NULL);
            _calculateUserPerceivedStalenessTime(firstNotVisibleItem, -1, timeGet,
                                                 stalenessStr);

            {
                std::lock_guard<std::mutex> lk(SysStats::NumFresherVersionsInItemChainMutex);
                SysStats::NumFresherVersionsInItemChain.push_back(numVersionsBehind);
            }
        }

        //Save number of unmerged versions
        if (numUnmergedVersions > 0) {
            std::lock_guard<std::mutex> lk(SysStats::NumUnmergedVersionsInItemChainMutex);
            SysStats::NumUnmergedVersionsInItemChain.push_back(numUnmergedVersions);
            SysStats::NumTimesReturnedUnmergedItemVersion++;
        }

        SysStats::NumReturnedItemVersions += 1;
#endif

        return next;
    }

#endif


#ifdef DEP_VECTORS

#else //SCALAR_YES

    ItemVersion *ItemAnchor::LatestSnapshotVersionOPT(PhysicalTimeSpec st) {

        std::vector<ItemVersion *> current;

        ItemVersion *next = NULL;
        PhysicalTimeSpec lst;

        LATEST_VERSION(current);

        while (true) {

            int nextIndex = 0;
            next = current[0];

            //Find freshest
            for (int i = 1; i < current.size(); ++i) {
                ItemVersion *cur = current[i];

                if (next == NULL || (cur != NULL && next < cur)) {
                    next = cur;
                    nextIndex = i;
                }
            }

            ASSERT(next != NULL);
            if (next->PDUT <= st)
                break;
            ASSERT(current[nextIndex] == next);
            current[nextIndex] = next->Next;
        }

        return next;
    }

#endif

    void ItemAnchor::MarkLocalUpdatePersisted(ItemVersion *version) {
        std::lock_guard<std::mutex> lk(_itemMutex);
        version->Persisted = true;
    }

    std::string ItemAnchor::ShowItemVersions() {
        std::string itemVersions = (boost::format("[key %s]") % _itemKey).str();

        std::vector<ItemVersion *> current;
        {
            std::lock_guard<std::mutex> lk(_itemMutex);
            current = _latestVersion;
        }

        ItemVersion *next;

        while (true) {

            int nextIndex = 0;
            next = current[0];

            for (int i = 1; i < current.size(); i++) {
                ItemVersion *cur = current[i];

                if (next == NULL || (cur != NULL && next < cur)) {
                    next = cur;
                    nextIndex = i;
                }

            }
            PhysicalTimeSpec timeNow = Utils::GetCurrentClockTime();
            if (next == NULL) break;

            itemVersions += (boost::format("->(%s, lut %d, put %s, rit %s, sr %d, timeNow %s)\n")
                             % next->Value
                             % next->LUT
                             % Utils::physicaltime2str(next->PUT)
                             % Utils::physicaltime2str(next->RIT)
                             % next->SrcReplica
                             % Utils::physicaltime2str(timeNow)

            ).str();

            ASSERT(current[nextIndex] == next);
            current[nextIndex] = next->Next;

        }

        return itemVersions;
    }


    inline ItemVersion *ItemAnchor::_getLatestItem() {

        std::vector<ItemVersion *> current;
        {
            std::lock_guard<std::mutex> lk(_itemMutex);
            current = _latestVersion;
        }

        ItemVersion *next = current[0];

        for (int i = 1; i < current.size(); ++i) {
            ItemVersion *cur = current[i];

            if (next == NULL || (cur != NULL && next < cur)) {
                next = cur;
            }
        }

        ASSERT(next != NULL);
        return next;
    }

    inline ItemVersion *ItemAnchor::_getLatestItemPess(int &nextIndex) {

        std::vector<ItemVersion *> current;
        {
            std::lock_guard<std::mutex> lk(_itemMutex);
            current = _latestVersion;
        }

        ItemVersion *next = current[0];

        for (int i = 1; i < current.size(); ++i) {
            ItemVersion *cur = current[i];

            if (next == NULL || (cur != NULL && next < cur)) {
                next = cur;
                nextIndex = i;
            }

        }

        ASSERT(next != NULL);
        return next;
    }


    inline bool ItemAnchor::_isGreaterOrEqual(std::vector<PhysicalTimeSpec> v1,
                                              std::vector<PhysicalTimeSpec> v2) {
        for (int i = 0; i < v1.size(); i++) {
            if (v1[i] < v2[i]) {
                return false;
            }
        }

        return true;
    }


    inline PhysicalTimeSpec ItemAnchor::_maxElem(std::vector<PhysicalTimeSpec> v) {

        PhysicalTimeSpec max = v[0];
        for (int i = 1; i < v.size(); i++) {
            if (v[i] > max) {
                max = v[i];
            }
        }
        return max;
    }

    inline bool ItemAnchor::allTrue(std::vector<bool> &v) {

        for (int i = 0; i < v.size(); i++) {
            if (v[i] == false) {
                return false;
            }
        }

        return true;
    }


#ifdef DEP_VECTORS

    ItemVersion *ItemAnchor::LatestCausalVersionPess(int localReplicaId, ConsistencyMetadata &cdata) {
        ItemVersion *next = NULL;
        ItemVersion *result_item = NULL;
        bool foundTheItem = false;
        std::vector<PhysicalTimeSpec> gsv = cdata.GSV;

#ifdef MEASURE_STATISTICS
        std::string stalenessStr = "";
        PhysicalTimeSpec timeGet = Utils::GetCurrentClockTime();
        ItemVersion *firstNotVisibleItem = NULL;
        bool stale = false;
        int numVersionsBehind = 0;
        int numUnmergedVersions = 0;
        std::vector<bool> firstVisiblePerReplicaFound; //[INFO: in the local replica everything is visible immediately]

        for (int i = 0; i < cdata.GSV.size(); i++) {
            firstVisiblePerReplicaFound.push_back(false);
        }
        firstVisiblePerReplicaFound[localReplicaId] = true;

#endif

        std::vector<ItemVersion *> current;
        LATEST_VERSION(current);

        while (true) {

            int nextIndex = 0;
            next = current[0];

            for (int i = 1; i < current.size(); ++i) {
                ItemVersion *cur = current[i];

                if (next == NULL || (cur != NULL && next < cur)) {
                    next = cur;
                    nextIndex = i;
                }
            }

            if (next == NULL) {
                //reached the end of the chain for every replica
                break;
            }
            ASSERT(next != NULL);

            //if the item is local
            if (next->SrcReplica == localReplicaId) {
                if (!foundTheItem) {
                    result_item = next;
                    foundTheItem = true;
                }
#ifndef MEASURE_STATISTICS
                break;
#else
                if (allTrue(firstVisiblePerReplicaFound)) {
                    break;
                }
#endif
            }

            bool vis = true;

            //Visible if all its DEPENDENCIES are stable.

            for (int j = 0; j < next->DV.size(); j++) {
                if (j == localReplicaId) { //No need to check dependencies on the local
                    continue;
                }

                if (next->DV[j] > gsv[j]) {
                    vis = false;
                    break;
                }
            }

            if (vis) {

                if (!foundTheItem) {
                    result_item = next;
                    foundTheItem = true;
                }

#ifndef MEASURE_STATISTICS
                break;
#else

                firstVisiblePerReplicaFound[next->SrcReplica] = true;
                if (allTrue(firstVisiblePerReplicaFound)) {
                    break;
                }
#endif
            }

#ifdef MEASURE_STATISTICS
            if (!foundTheItem) {
                stale = true;
                firstNotVisibleItem = next;
                numVersionsBehind++;
            }

            if (!vis) { numUnmergedVersions++; }
#endif

            if (!foundTheItem) {
                current[nextIndex] = next->Next;
            } else {
#ifdef MEASURE_STATISTICS
                if (allTrue(firstVisiblePerReplicaFound)) {
                    break;
                } else {
                    current[nextIndex] = next->Next;
                }
#else
                break;
#endif

            }
        }

#ifdef MEASURE_STATISTICS

        if (stale) {
            SysStats::NumReturnedStaleItemVersions++;

            ASSERT(firstNotVisibleItem != NULL);
            _calculateUserPerceivedStalenessTime(firstNotVisibleItem, -1, timeGet, stalenessStr);

            {
                std::lock_guard<std::mutex> lk(SysStats::NumFresherVersionsInItemChainMutex);
                SysStats::NumFresherVersionsInItemChain.
                        push_back(numVersionsBehind);
            }
        }

        if (numUnmergedVersions > 0) {
            std::lock_guard<std::mutex> lk(SysStats::NumUnmergedVersionsInItemChainMutex);
            SysStats::NumUnmergedVersionsInItemChain.push_back(numUnmergedVersions);
            SysStats::NumTimesReturnedUnmergedItemVersion++;
        }

        SysStats::NumReturnedItemVersions += 1;
#endif

        if (result_item->SrcReplica == localReplicaId) {
            SysStats::NumReturnedLocalItemVersions++;
        } else {
            SysStats::NumReturnedRemoteItemVersions++;
        }

        return result_item;
    }

// #################################################################
    ItemVersion *ItemAnchor::LatestCausalVersionPess2(int localReplicaId, ConsistencyMetadata &cdata) {
        ItemVersion *next = NULL;
        ItemVersion *result_item = NULL;
        std::vector<PhysicalTimeSpec> gsv = cdata.GSV;

#ifdef MEASURE_STATISTICS
        std::string stalenessStr = "";
        PhysicalTimeSpec timeGet = Utils::GetCurrentClockTime();
        ItemVersion *firstNotVisibleItem = NULL;
        bool stale = false;
        int numVersionsBehind = 0;
        int numUnmergedVersions = 0;
#endif

        std::vector<ItemVersion *> current;
        LATEST_VERSION(current);
        //Find the fist visible item
            while (true) {
                int nextIndex = 0;
                next = current[0];

                for (int i = 1; i < current.size(); ++i) {
                    ItemVersion *cur = current[i];

                    if (next == NULL || (cur != NULL && next < cur)) {
                        next = cur;
                        nextIndex = i;
                    }
                }

                ASSERT(next != NULL);
                if (next->SrcReplica == localReplicaId) {
                    break;
                }

                bool vis = true;

                //Visible if all its DEPENDENCIES are stable.

                for (int j = 0; j < next->DV.size(); j++) {
                    if (j == localReplicaId) { //No need to check dependencies on the local
                        continue;
                    }

                    if (next->DV[j] > gsv[j]) {
                        vis = false;
                        break;
                    }
                }

                if (vis) {
                    break;
                }

#ifdef MEASURE_STATISTICS
                stale = true;
                firstNotVisibleItem = next;
                numVersionsBehind++;
                numUnmergedVersions++;
#endif
                current[nextIndex] = next->Next;
            }

            result_item = next;

#ifdef MEASURE_STATISTICS
            //Search for unmerged items for every replica
            for (int i = 0; i < current.size(); i++) {
                if (i == localReplicaId) {
                    continue;
                }

                ItemVersion *chainItem = current[i];

                while (true) {
                    if (chainItem == NULL) {
                        //The end of the chain is reached
                        break;
                    } else {
                        //Check visibility of the item
                        bool isVisible = true;
                        for (int j = 0; j < chainItem->DV.size(); j++) {
                            if (j == localReplicaId) { //Items from local replica are always visible
                                continue;
                            }

                            if (chainItem->DV[j] > gsv[j]) {
                                isVisible = false;
                                numUnmergedVersions++;
                                break;
                            }
                        }

                        if (isVisible) {
                            break;
                        } else {
                            chainItem = chainItem->Next;
                        }
                    }
                }
            }
#endif
#ifdef MEASURE_STATISTICS

        if (stale) {
            SysStats::NumReturnedStaleItemVersions++;

            ASSERT(firstNotVisibleItem != NULL);
            _calculateUserPerceivedStalenessTime(firstNotVisibleItem, -1, timeGet,
                                                 stalenessStr);

            {
                std::lock_guard<std::mutex> lk(SysStats::NumFresherVersionsInItemChainMutex);
                SysStats::NumFresherVersionsInItemChain.push_back(numVersionsBehind);
            }
        }

        //Save number of unmerged versions
        if (numUnmergedVersions > 0) {
            std::lock_guard<std::mutex> lk(SysStats::NumUnmergedVersionsInItemChainMutex);
            SysStats::NumUnmergedVersionsInItemChain.push_back(numUnmergedVersions);
            SysStats::NumTimesReturnedUnmergedItemVersion++;
        }

        SysStats::NumReturnedItemVersions += 1;

        if (result_item->SrcReplica == localReplicaId) {
            SysStats::NumReturnedLocalItemVersions++;
        } else {
            SysStats::NumReturnedRemoteItemVersions++;
        }
#endif

    return result_item;
    }

#endif //DEP_VECTORS

} // namespace scc
