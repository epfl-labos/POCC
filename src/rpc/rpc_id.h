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


#ifndef SCC_RPC_RPC_ID_H
#define SCC_RPC_RPC_ID_H

namespace scc {

    enum class RPCMethod {
        // key-value server
                EchoTest = 1,
        GetServerConfig,
        Add,
        Get,
        Set,
        TxGet,
        ShowItem, // show the version chain of one key
        ShowDB, // show the version chains of all items
        ShowState,
        DumpLatencyMeasurement,

        // partiton
        InternalGet,
        InternalSet,
        InternalTxSliceGet,
        InternalShowItem,
        InitializePartitioning,
        InternalDependencyCheck,

        // replication,
        InitializeReplication,
        ReplicateUpdate,
        SendHeartbeat,
        // GST
                SendLST,
        SendGST,
#ifdef DEP_VECTORS
        SendPVV,
      SendGSV,
      SendGSVRequest,
#endif

#ifdef PARALLEL_XACTS
        ParallelInternalTxSliceGetResult,
#endif

        // manager
        RegisterPartition,
        GetRegisteredPartitions,
        NotifyReadiness

    };

    static const char *RPCMethodS[] = {
            "EchoTest",
            "GetServerConfig",
            "Add",
            "Get",
            "Set",
            "TxGet",
            "ShowItem",
            "ShowDB",
            "ShowState",
            "DumpLatencyMeasurement",

            "InternalGet",
            "InternalSet",
            "InternalTxSliceGet",
            "InternalShowItem",
            "InitializePartitioning",
            "InternalDependencyCheck",

            "InitializeReplication",
            "ReplicateUpdate",
            "SendHeartbeat",
            "SendLST",
            "SendGST",
#ifdef DEP_VECTORS
            "SendPVV",
            "SendGSV",
            "SendGSVRequest",
#endif

#ifdef PARALLEL_XACTS
            "ParallelInternalTxSliceGetResult",
#endif
            "RegisterPartition",
            "GetRegisteredPartitions",
            "NotifyReadiness"
    };

    const char *getTextForEnum(RPCMethod rid) {
        return RPCMethodS[((int) rid) - 1];
    }

}

#endif
