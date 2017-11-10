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

#include "parallel_xact.h"

namespace scc {

    ParallelXact::ParallelXact(unsigned    long id,    int toDo    ):
            xact_id (id),
            to_do(toDo),
            _key_to_part(toDo),
            _waitHandle(toDo){
                for(int i=0;i<toDo;i++){
                    _slices.push_back(new ParallelXactSlice());
                }
    }

    int ParallelXact::decrementGetAndSignalIfNecessary() {
        std::lock_guard <std::mutex> lk(_mutex);
        return --to_do;
    }

}