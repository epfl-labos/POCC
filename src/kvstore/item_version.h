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


#ifndef SCC_KVSTORE_ITEM_VERSION_H_
#define SCC_KVSTORE_ITEM_VERSION_H_

#include "common/utils.h"
#include <string>
#include <boost/format.hpp>

namespace scc {

    class ItemVersion {
    public:
        std::string Value;
        int64_t LUT; // logical update time
        PhysicalTimeSpec PUT; // physical update time
        PhysicalTimeSpec PDUT; // item dependency physical update time
        int SrcReplica;
        int SrcPartition;
        bool Persisted;
        ItemVersion *Next;
        bool blocked;
        // visibility measurement
        PhysicalTimeSpec RIT; //replica install time
        std::string Key;

#ifdef DEP_VECTORS
        std::vector<PhysicalTimeSpec> DV;
#endif
    public:

        ItemVersion(const std::string &value) {
            Value = value;
            Next = NULL;
        }

        std::string ShowItemVersion();
    };

    std::string ItemVersion::ShowItemVersion() {
        std::string versionStr;

        versionStr = (boost::format("(lut %d, src replica %d, persisted %s)")
                      % LUT
                      % SrcReplica
                      % (Persisted ? "true" : "false")
        ).str();

        return versionStr;
    }

    bool operator==(const ItemVersion &a, const ItemVersion &b) {
        return (a.PUT == b.PUT) &&
               (a.SrcReplica == b.SrcReplica);
    }

    bool operator>(const ItemVersion &a, const ItemVersion &b) {
        if (a.PUT > b.PUT) {
            return true;
        } else if (a.PUT == b.PUT) {
            if (a.SrcReplica > b.SrcReplica) {
                return true;
            }
        }

        return false;
    }

    bool operator<=(const ItemVersion &a, const ItemVersion &b) {
        return !(a > b);
    }

    bool operator<(const ItemVersion &a, const ItemVersion &b) {
        if (a.PUT < b.PUT) {
            return true;
        } else if (a.PUT == b.PUT) {
            if (a.SrcReplica < b.SrcReplica) {
                return true;
            }
        }

        return false;
    }

    bool operator>=(const ItemVersion &a, const ItemVersion &b) {
        return !(a < b);
    }

} // namespace scc

#endif
