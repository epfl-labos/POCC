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


#ifndef SCC_COMMON_UTILS_H
#define SCC_COMMON_UTILS_H

#include "common/types.h"
#include "common/sys_config.h"
#include "common/sys_logger.h"
#include <time.h>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <libgen.h>
#include <sys/param.h>
#include <pthread.h>
#include <sys/syscall.h>
#include <thread>
#include <assert.h>
#include <functional>
#include <random>
#include <boost/format.hpp>
#include <boost/thread.hpp>
#include <iostream>
#include <stdio.h>
#include <stdint.h>

namespace scc {

#define MAX(a, b) (a > b ? a : b)
#define MIN(a, b) (a < b ? a : b)

#ifdef USE_ASSERT
#define ASSERT(A) assert(A)
#else
#define ASSERT(A)
#endif

    class Utils {
    public:
        static std::string GetHostName();

        static std::string GetCurrentExecFileName();

        static unsigned int GetThreadId();

        static int rand(int min, int max);

        static double rand(int decimals, double min, double max);

        static int strhash(const std::string &str);

        static PhysicalTimeSpec GetCurrentClockTime();

        static std::string logicaltv2str(const LogicalTimeVector &tv);

        static std::string logicaltvv2str(const LogicalTimeVectorVector &tvv);

        static std::string physicaltime2str(const PhysicalTimeSpec &time);

        static std::string physicaltv2str(const PhysicalTimeVector &tv);

        static std::string physicaltvv2str(const PhysicalTimeVectorVector &tvv);

        static ConsistencyType str2consistency(char *cstr);

        static PhysicalTimeSpec max(PhysicalTimeSpec t1, PhysicalTimeSpec t2);

        static bool isGreater(std::vector <PhysicalTimeSpec> v1, std::vector <PhysicalTimeSpec> v2);

        static bool isGreaterOrEqual(std::vector <PhysicalTimeSpec> v1, std::vector <PhysicalTimeSpec> v2);

        static PhysicalTimeSpec maxElem(std::vector <PhysicalTimeSpec> v);

        static double RandomDouble(double min, double max);
    };

    double Utils::RandomDouble(double min = 0.0, double max = 1.0) {
        static std::default_random_engine generator;
        static std::uniform_real_distribution<double> uniform(min, max);
        return uniform(generator);
    }

    int Utils::rand(int min, int max) {
        ASSERT(max >= min);

        int value = random();
        if (min == max) {
            value = value % min;
        } else {
            value = value % (max - min + 1) + min;
        }

        return value;
    }

    double Utils::rand(int decimals, double min, double max) {
        ASSERT(max >= min);
        ASSERT(decimals > 0);

        int multiplier = 1;
        for (int i = 0; i < decimals; i++) {
            multiplier *= 10;
        }

        int intMin = static_cast<int> (min * multiplier);
        int intMax = static_cast<int> (max * multiplier);

        int rnd = random();
        if (intMin == intMax) {
            rnd = rnd % intMin;
        } else {
            rnd = rnd % (intMax - intMin + 1) + intMin;
        }

        return rnd / static_cast<double> (multiplier);
    }

    std::string Utils::GetHostName() {
        // max length of hostname decided at kernel compilation time
        char buf[128];
        gethostname(buf, 128);
        return std::string(buf);
    }

    std::string Utils::GetCurrentExecFileName() {
        char buf[MAXPATHLEN];
        int len = readlink("/proc/self/exe", buf, MAXPATHLEN);
        buf[len] = '\0';
        return std::string(basename(buf));
    }

    unsigned int Utils::GetThreadId() {
        return (unsigned int) syscall(SYS_gettid);
    }

    int Utils::strhash(const std::string &str) {
        // size_t -> int
        int hash = std::hash<std::string>()(str);
        return abs(hash);
    }

#ifdef SIXTY_FOUR_BIT_CLOCK

#define TO_64(A, pts) do{\
                    if(CHECK_BIT(pts.Seconds,32)){\
                        assert(false);}\
                   A.Seconds = pts.Seconds &  0x00000000FFFFFFFF;\
                   A.Seconds = A.Seconds << 32;\
                   A.Seconds |= (pts.NanoSeconds << 2);\
                   A.NanoSeconds = 0;\
                   }while(0)

#define FROM_64(A,pts) do{\
                     A.NanoSeconds = ((pts.Seconds & 0x00000000FFFFFFFF) >> 2);\
                     A.Seconds = pts.Seconds >> 32;\
                     }while(0)
#else
#define TO_64(A, pts) A = pts;
#define FROM_64(A, pts) A = pts;
#endif

#define CHECK_BIT(var,pos) ((var) & (1<<(pos)))

    PhysicalTimeSpec Utils::GetCurrentClockTime() {
        PhysicalTimeSpec pts;
        timespec ts;

        clock_gettime(CLOCK_REALTIME, &ts);
        pts.Seconds = ts.tv_sec;

        pts.NanoSeconds = ts.tv_nsec;
        return pts;
    }

    std::string Utils::logicaltv2str(const LogicalTimeVector &tv) {
        std::string str;

        str = "[ ";
        for (unsigned int i = 0; i < tv.size(); i++) {
            str += std::to_string(tv[i]) + " ";
        }
        str += "]";

        return str;
    }

    std::string Utils::logicaltvv2str(const LogicalTimeVectorVector &tvv) {
        std::string str;

        str = "[ ";
        for (unsigned int i = 0; i < tvv.size(); i++) {
            str += "[ ";
            for (unsigned int j = 0; j < tvv[i].size(); j++) {
                str += std::to_string(tvv[i][j]) + " ";
            }
            str += "] ";
        }
        str += "]";

        return str;
    }

    std::string Utils::physicaltime2str(const PhysicalTimeSpec &time) {
        std::string timeStr;
        timeStr += std::to_string(time.Seconds) + ".";
        timeStr += std::to_string(time.NanoSeconds / 1000000000.0).substr(2);
        return timeStr;
    }

    std::string Utils::physicaltv2str(const PhysicalTimeVector &tv) {
        std::string str;

        str = "[ ";
        for (unsigned int i = 0; i < tv.size(); i++) {
            str += physicaltime2str(tv[i]) + " ";
        }
        str += "]";

        return str;
    }

    std::string Utils::physicaltvv2str(const PhysicalTimeVectorVector &tvv) {
        std::string str;

        str = "[ ";
        for (unsigned int i = 0; i < tvv.size(); i++) {
            str += "[ ";
            for (unsigned int j = 0; j < tvv[i].size(); j++) {
                str += physicaltime2str(tvv[i][j]) + " ";
            }
            str += "]";
        }
        str += "]";

        return str;
    }

    ConsistencyType Utils::str2consistency(char *cstr) {
        std::string str = cstr;
        if (str == "Causal") {
            return ConsistencyType::Causal;
        }

        assert(false);
    }

    PhysicalTimeSpec Utils::max(PhysicalTimeSpec t1, PhysicalTimeSpec t2) {
        if (t1 >= t2) {
            return t1;
        } else {
            return t2;
        }
    }

    bool Utils::isGreater(std::vector <PhysicalTimeSpec> v1, std::vector <PhysicalTimeSpec> v2) {

        bool ret = true;
        ASSERT(v1.size() == v2.size());

        for (int i = 0; i < v1.size(); i++) {
            if (v1[i] <= v2[i]) {
                return false;
            }
        }
        return true;
    }

    bool Utils::isGreaterOrEqual(std::vector <PhysicalTimeSpec> v1, std::vector <PhysicalTimeSpec> v2) {

        bool ret = true;
        ASSERT(v1.size() == v2.size());

        for (int i = 0; i < v1.size(); i++) {
            if (v1[i] < v2[i]) {
                return false;
            }
        }
        return true;
    }

    PhysicalTimeSpec Utils::maxElem(std::vector <PhysicalTimeSpec> v) {
        ASSERT(v.size() > 0);
        PhysicalTimeSpec max = v[0];
        for (int i = 1; i < v.size(); i++) {
            if (v[i] > max) {
                max = v[i];
            }
        }
        return max;
    }

} // namespace scc

#endif
