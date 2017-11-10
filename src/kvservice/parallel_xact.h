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

#ifndef GRAIN_PARALLEL_XACT_H
#define GRAIN_PARALLEL_XACT_H

#include "messages/rpc_messages.pb.h"
#include "common/types.h"


#define GET_SLEEP(px, index) (px._slices[index]->metadata->waited_xact)
#define GET_RET(px, index) (px._slices[index]->ret)
#define GET_SRC_REPLICA(px,index) (px._slices[index]->metadata->SrcReplica)
#define GET_PART_TO_KEY(px,part) (px->_part_to_keys[part])
#define GET_VALUE(px,index)  (px._slices[index]->value)
#define GET_DT(px,index) (px._slices[index]->metadata->DT)
#define INITIALIZE_METADATA(px,cdata,index)  (px._slices[index]->metadata= cdata)
#define SET_KEY_TO_PART(px,key,part)(px._key_to_part[key] = part)
#define SET_PART_TO_KEY(px,part,key)(px._part_to_keys.insert(std::pair<int,int>(part,key)))
#ifdef DEP_VECTORS
#define STORE_RESULT(px,index,opResult) do{\
                                                ParallelXactSlice * slice = px->_slices[index];\
                                                slice->ret = opResult.succeeded();\
                                                slice->value = opResult.getvalue();\
                                                slice->metadata->DT.Seconds = opResult.dt().seconds();\
                                                slice->metadata->DT.NanoSeconds = opResult.dt().nanoseconds();\
                                                slice->metadata->SrcReplica = opResult.srcreplica();\
                                                slice->metadata->waited_xact = opResult.waitedxact();\
                                            }   while(0);
#define STORE_RESULTS(px,index,v,put,srcReplica) do{\
                                                        ParallelXactSlice* slice = px._slices[index];\
                                                        slice->ret = true;\
                                                        slice->value = v;\
                                                        slice->metadata->DT.Seconds = put.Seconds;\
                                                        slice->metadata->DT.NanoSeconds = put.NanoSeconds;\
                                                        slice->metadata->SrcReplica = srcReplica;\
                                                    }while(0);
#else
#define STORE_RESULT(px,index,opResult) do{\
                                                ParallelXactSlice * slice = px->_slices[index];\
                                                slice->ret = opResult.succeeded();\
                                                slice->value = opResult.getvalue();\
                                                slice->metadata->DT.Seconds = opResult.dt().seconds();\
                                                slice->metadata->DT.NanoSeconds = opResult.dt().nanoseconds();\
                                                slice->metadata->waited_xact = opResult.waitedxact();\
                                            }   while(0);
#define STORE_RESULTS(px,index,v,put,srcReplica) do{\
                                                        ParallelXactSlice* slice = px._slices[index];\
                                                        slice->ret = true;\
                                                        slice->value = v;\
                                                        slice->metadata->DT.Seconds = put.Seconds;\
                                                        slice->metadata->DT.NanoSeconds = put.NanoSeconds;\
                                                    }while(0);
#endif //DEP_VECTORS

namespace scc {
    class ParallelXactSlice;

    class ParallelXact{
        public:
            ParallelXact(unsigned    long id,    int toDo   );
            std::vector<int> _key_to_part;
            std::unordered_map<int, int> _part_to_keys;
            unsigned long xact_id;
            int to_do;
            std::mutex _mutex;
            WaitHandle _waitHandle;
            std::vector<ParallelXactSlice*> _slices;

            int decrementGetAndSignalIfNecessary();
            std::string GetValue(int index);
            PhysicalTimeSpec GetDT(int index);

        ~ParallelXact(){
            for(int i=0;i<_slices.size();i++)
                delete _slices[i];
        }

    };


    class ParallelXactSlice{
    public:
        ConsistencyMetadata* metadata;
        std::string value;
        bool ret;

    public:
        ParallelXactSlice(){ret = false;};
        ~ParallelXactSlice(){
            delete metadata;
        }
    };
}

#endif //GRAIN_PARALLEL_XACT_H
