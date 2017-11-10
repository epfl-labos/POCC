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


#ifndef SCC_COMMON_TYPES_H
#define SCC_COMMON_TYPES_H

#include "common/wait_handle.h"
#include "common/sys_logger.h"
#include <sys/types.h>
#include <string>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <functional>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cmath>
#include <cassert>
#include <inttypes.h>

namespace scc {

    class ServerConfig {
    public:
        int NumPartitions;
        int NumReplicasPerPartition;
    };

    class Configuration {
    public:
        int NumPartitions;
        int NumReplicasPerPartition;
        int TotalNumItems;
        int NumValueBytes;
        int NumOpsPerThread;
        int NumThreads;
        int ServingReplicaId;
        int ServingPartitionId;
        int ClientServerId;
        std::string RequestsDistribution;
        double RequestsDistributionParameter;
        std::string GroupServerName;
        int GroupServerPort;
        int ReadRatio;
        int WriteRatio;
        std::string clientServerName;
        std::string latencyOutputFileName;
        bool reservoirSampling;
        int reservoirSamplingLimit;
        int experimentDuration; //milliseconds
        int TotalNumOps;
        int experimentType; //1- experiment1, 2 -experiment2
        int clientResetNumber;
        bool enableClientReset;
        int numTxReadItems;
        int locality;
        int thinkTime;
    };


    class PhysicalTimeSpec {
    public:

        PhysicalTimeSpec() : Seconds(0), NanoSeconds(0) {
        }

        PhysicalTimeSpec(int64_t seconds, int64_t nanoSeconds)
                : Seconds(seconds),
                  NanoSeconds(nanoSeconds) {
        }

        bool Zero() {
            return Seconds == 0 && NanoSeconds == 0;
        }

        int64_t Seconds;
        int64_t NanoSeconds;

        double toMilliSeconds();

        double toSeconds();

        void setToZero() {
            Seconds = 0;
            NanoSeconds = 0;
        }

    };

    bool operator==(const PhysicalTimeSpec &a, const PhysicalTimeSpec &b) {
        return (a.Seconds == b.Seconds) && (a.NanoSeconds == b.NanoSeconds);
    }

    bool operator!=(const PhysicalTimeSpec &a, const PhysicalTimeSpec &b) {
        return !(a == b);
    }

    bool operator>(const PhysicalTimeSpec &a, const PhysicalTimeSpec &b) {
        return (a.Seconds > b.Seconds) ||
               ((a.Seconds == b.Seconds) && (a.NanoSeconds > b.NanoSeconds));
    }

    bool operator<=(const PhysicalTimeSpec &a, const PhysicalTimeSpec &b) {
        return !(a > b);
    }

    bool operator<(const PhysicalTimeSpec &a, const PhysicalTimeSpec &b) {
        return (a.Seconds < b.Seconds) ||
               ((a.Seconds == b.Seconds) && (a.NanoSeconds < b.NanoSeconds));
    }

    bool operator>=(const PhysicalTimeSpec &a, const PhysicalTimeSpec &b) {
        return !(a < b);
    }

    PhysicalTimeSpec operator-(PhysicalTimeSpec &a, PhysicalTimeSpec &b) {
        PhysicalTimeSpec d;

        d.Seconds = a.Seconds - b.Seconds;
        d.NanoSeconds = a.NanoSeconds - b.NanoSeconds;

        while (d.NanoSeconds < 0) {
            d.Seconds -= 1;
            d.NanoSeconds += 1000000000;
        }

        return d;
    }

    double PhysicalTimeSpec::toMilliSeconds() {
        double res = 0;

        res = (double) this->NanoSeconds / 1000000.0;
        res += this->Seconds * 1000.0;

        return res;
    }

    double PhysicalTimeSpec::toSeconds() {
        double res = 0;

        res = this->Seconds + (double) this->NanoSeconds / 1000000000.0;

        return res;
    }

    enum class DurabilityType {
        Disk = 1,
        Memory
    };

    enum class GSTDerivationType {
        TREE = 1,
        BROADCAST,
        SIMPLE_BROADCAST
    };

    class DBPartition {
    public:

        DBPartition()
                : Name("NULL"),
                  PublicPort(-1),
                  PartitionPort(-1),
                  ReplicationPort(-1),
                  PartitionId(-1),
                  ReplicaId(-1) {
        }

        DBPartition(std::string name, int publicPort)
                : Name(name),
                  PublicPort(publicPort),
                  PartitionPort(-1),
                  ReplicationPort(-1),
                  PartitionId(-1),
                  ReplicaId(-1) {
        }

        DBPartition(std::string name,
                    int publicPort,
                    int partitionPort,
                    int replicationPort,
                    int partitionId,
                    int replicaId)
                : Name(name),
                  PublicPort(publicPort),
                  PartitionPort(partitionPort),
                  ReplicationPort(replicationPort),
                  PartitionId(partitionId),
                  ReplicaId(replicaId) {
        }

    public:
        std::string Name;
        int PublicPort;
        int PartitionPort;
        int ReplicationPort;
        int PartitionId;
        int ReplicaId;
    };

    typedef std::vector<int64_t> LogicalTimeVector;
    typedef std::vector<std::vector<int64_t>> LogicalTimeVectorVector;

    typedef std::vector<PhysicalTimeSpec> PhysicalTimeVector;
    typedef std::vector<std::vector<PhysicalTimeSpec>> PhysicalTimeVectorVector;

    bool operator<(const LogicalTimeVectorVector &a,
                   const LogicalTimeVectorVector &b) {
        bool leq = true;
        bool strictLess = false;
        for (unsigned int i = 0; i < a.size(); i++) {
            for (unsigned int j = 0; j < a[i].size(); j++) {
                if (a[i][j] < b[i][j]) {
                    strictLess = true;
                } else if (a[i][j] == b[i][j]) {
                    // do nothing
                } else {
                    leq = false;
                    break;
                }
            }
        }

        return (leq && strictLess);
    }

    bool operator>(const LogicalTimeVectorVector &a,
                   const LogicalTimeVectorVector &b) {
        bool geq = true;
        bool strictGreater = false;
        for (unsigned int i = 0; i < a.size(); i++) {
            for (unsigned int j = 0; j < a[i].size(); j++) {
                if (a[i][j] > b[i][j]) {
                    strictGreater = true;
                } else if (a[i][j] == b[i][j]) {
                    // do nothing
                } else {
                    geq = false;
                    break;
                }
            }
        }

        return (geq && strictGreater);
    }

    enum class ConsistencyType {
        Causal = 1
    };

    class ReadItemVersion {
    public:
        std::string Value;
        int64_t LUT;
        int SrcReplica;
        int SrcPartition;
    };

    class GetTransaction {
    public:
        std::vector<std::string> ToReadKeys;
        std::vector<ReadItemVersion> ReadVersions;
    };

    class ConsistencyMetadata {
    public:
        PhysicalTimeSpec DT;
        PhysicalTimeSpec GST;
        PhysicalTimeSpec DUT;
        PhysicalTimeSpec minLST;
        double waited_xact;

#ifdef DEP_VECTORS
        std::vector<PhysicalTimeSpec> NDV;
        std::vector<PhysicalTimeSpec> DV;
        int SrcReplica; //Source replica id
        int MaxElId;
        std::vector<PhysicalTimeSpec> GSV;
#endif
    };

    class PropagatedUpdate {
    public:

        PropagatedUpdate()
                : UpdatedItemAnchor(NULL),
                  UpdatedItemVersion(NULL) {
        }

        std::string SerializedRecord;
        std::string Key;
        std::string Value;
        //int64_t LUT;
        PhysicalTimeSpec PUT;

        #ifdef DEP_VECTORS
        std::vector<PhysicalTimeSpec> NDV;
        std::vector<PhysicalTimeSpec> DV;
#else
        PhysicalTimeSpec PDUT;
#endif //DEP_VECTORS

        int SrcPartition;
        int SrcReplica;
        void *UpdatedItemAnchor;
        void *UpdatedItemVersion;

#ifdef MINLSTKEY
        std::string minLSTKey;
#endif //MINLSTKEY
    };

    class Heartbeat {
    public:
        PhysicalTimeSpec PhysicalTime;
        int64_t LogicalTime;
    };

    class LocalUpdate {
    public:

        LocalUpdate()
                : UpdatedItemAnchor(NULL),
                  UpdatedItemVersion(NULL) {
        }

        void *UpdatedItemAnchor;
        void *UpdatedItemVersion;
        std::string SerializedRecord;
        bool delayed;
    };

    class UpdatedItemVersion {
    public:
        int64_t LUT;
        int SrcReplica;
    };

    typedef struct {

        long operator()(const UpdatedItemVersion &v) const {
            return (v.LUT << 4) + (v.SrcReplica);
        }
    } UpdatedVersionHash;

    typedef struct {

        long operator()(const UpdatedItemVersion &a, const UpdatedItemVersion &b) const {
            return (a.LUT == b.LUT) && (a.SrcReplica == b.SrcReplica);
        }
    } UpdatedVersionEq;

    typedef std::unordered_map<UpdatedItemVersion,
            PropagatedUpdate *,
            UpdatedVersionHash,
            UpdatedVersionEq> WaitingPropagatedUpdateMap;

    class ReplicationLatencyResult {
    public:
        PhysicalTimeSpec OverallLatency;
        PhysicalTimeSpec PropagationLatency;
        PhysicalTimeSpec VisibilityLatency;
        int SrcReplica;
    };

    enum class WorkloadType {
        READ_ALL_WRITE_LOCAL = 1,
        READ_WRITE_RATIO,
        WRITE_ROUND_ROBIN,
        RANDOM_READ_WRITE
    };

    class StalenessTimeMeasurement {
    public:
        StalenessTimeMeasurement() : sumTimes(0.0), count(0) {}

        double sumTimes;
        int count;
    };

    class VisibilityStatistics {
    public:
        double overall[12];
        double propagation[12];
        double visibility[12];

        VisibilityStatistics() {
            int i;
            for (i = 0; i < 12; i++) { overall[i] = -1; }
            for (i = 0; i < 12; i++) { propagation[i] = -1; }
            for (i = 0; i < 12; i++) { visibility[i] = -1; }
        }

    };

    class StalenessStatistics {
    public:
        double averageStalenessTime;
        double averageStalenessTimeTotal;
        double maxStalenessTime;
        double minStalenessTime;
        double averageUserPerceivedStalenessTime;
        double averageFirstVisibleItemVersionStalenessTime;
        double averageNumFresherVersionsInItemChain;
        double averageNumFresherVersionsInItemChainTotal;
        double medianStalenessTime;
        double _90PercentileStalenessTime;
        double _95PercentileStalenessTime;
        double _99PercentileStalenessTime;

        StalenessStatistics() : minStalenessTime(1000000.0),
                                maxStalenessTime(0.0),
                                averageStalenessTime(0.0),
                                averageFirstVisibleItemVersionStalenessTime(0.0),
                                averageNumFresherVersionsInItemChain(0.0),
                                averageNumFresherVersionsInItemChainTotal(0.0),
                                averageUserPerceivedStalenessTime(0.0),
                                averageStalenessTimeTotal(0.0),
                                medianStalenessTime(0.0),
                                _90PercentileStalenessTime(0.0),
                                _95PercentileStalenessTime(0.0),
                                _99PercentileStalenessTime(0.0) {}
    };

    class Statistics {
    public:
        std::string type;
        double average;
        double median;
        double min;
        double max;
        double _75Percentile;
        double _90Percentile;
        double _95Percentile;
        double _99Percentile;
        double variance;
        double standardDeviation;

        Statistics() : average(0.0),
                       median(0.0),
                       min(100000000.0),
                       max(0.0),
                       _75Percentile(0.0),
                       _90Percentile(0.0),
                       _95Percentile(0.0),
                       _99Percentile(0.0),
                       variance(0.0),
                       standardDeviation(0.0),
                       type("") {}

        void setToZero() {
            average = 0;
            median = 0;
            _75Percentile = 0;
            _99Percentile = 0;
            _95Percentile = 0;
            _90Percentile = 0;
            variance = 0;
            standardDeviation = 0;
            min = 0;
            max = 0;
        }

        std::string toString() {
            std::string
                    str = (boost::format("average %s %.5lf\n "
                                                 "median %s %.5lf\n"
                                                 "min %s %.5lf\n"
                                                 "max %s %.5lf\n"
                                                 "75 percentile %s %.5lf\n"
                                                 "90 percentile %s %.5lf\n"
                                                 "95 percentile %s %.5lf\n"
                                                 "99 percentile %s %.5lf\n")
                           % type % average
                           % type % median
                           % type % min
                           % type % max
                           % type % _75Percentile
                           % type % _90Percentile
                           % type % _95Percentile
                           % type % _99Percentile).str();

            return str;

        }
    };

    class OptVersionBlockStatistics : public Statistics {
    public:
        OptVersionBlockStatistics() : Statistics() {}
    };

    class BlockDurationPercentilesStatistics {
    public:
        double percentiles[12];

        BlockDurationPercentilesStatistics() {
            int i;
            for (i = 0; i < 12; i++) { percentiles[i] = -1; }
        }

    };

    class UnmergedVersionsStatistics : public Statistics {
    public:
        double percentiles[12];
        double value;

        UnmergedVersionsStatistics() : Statistics() {
            for (int i = 0; i < 12; i++) { percentiles[i] = -1; }
            value = 0;
        }

        void setToZero() {
            this->setToZero();
            for (int i = 0; i < 12; i++) { percentiles[i] = -1; }
            value = 0;
        }

    };


} // namespace scc

#endif
