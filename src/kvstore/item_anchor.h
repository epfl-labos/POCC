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


#ifndef SCC_KVSTORE_ITEM_ANCHOR_
#define SCC_KVSTORE_ITEM_ANCHOR_

#include "kvstore/item_version.h"
#include "common/types.h"
#include <thread>
#include <list>
#include <utility>

namespace scc {

    class ItemAnchor {
    public:
        ItemAnchor(std::string itemKey, int numReplicas);

        ~ItemAnchor();

        void InsertVersion(ItemVersion *version);
        static int _replicaId;
#ifdef DEP_VECTORS
        ItemVersion *LatestCausalVersion(int localReplicaId, ConsistencyMetadata& cdata);
        ItemVersion *LatestCausalVersionOpt(int localReplicaId, ConsistencyMetadata& cdata);
        ItemVersion *LatestCausalVersionPess(int localReplicaId, ConsistencyMetadata& cdata);
        ItemVersion *LatestCausalVersionPess2(int localReplicaId, ConsistencyMetadata& cdata);
#else


        ItemVersion *LatestCausalVersion(int localReplicaId, PhysicalTimeSpec &t);
        ItemVersion *LatestCausalVersionOpt(int localReplicaId, PhysicalTimeSpec &t);
        ItemVersion *LatestCausalVersionPess(int localReplicaId, PhysicalTimeSpec &t);

#endif


#ifndef DEP_VECTORS

        ItemVersion *LatestSnapshotVersion(PhysicalTimeSpec st);
        ItemVersion *LatestSnapshotVersionOPT(PhysicalTimeSpec st);

#else
        ItemVersion *LatestSnapshotVersion(std::vector<PhysicalTimeSpec> &st, int localReplicaId);

#endif

        void MarkLocalUpdatePersisted(ItemVersion *version);

        std::string ShowItemVersions();

        std::string _itemKey;
    private:

        std::mutex _itemMutex;
        std::vector<ItemVersion *> _latestVersion;
        ItemVersion * _lastAddedVersion;

        void _calculateUserPerceivedStalenessTime(ItemVersion *firstNotVisibleItem, int replicaId,
                                                  PhysicalTimeSpec timeGet, std::string stalenessStr);

        bool isHot(std::string basic_string);

        inline ItemVersion *_getLatestItem();

        inline ItemVersion *_getLatestItemPess(int &nextIndex);

        inline bool _isGreaterOrEqual(std::vector <PhysicalTimeSpec> v1, std::vector <PhysicalTimeSpec> v2);

        inline PhysicalTimeSpec _maxElem(std::vector <PhysicalTimeSpec> v);

        inline bool allTrue(std::vector<bool> &v);
    };

} // namespace scc

#endif
