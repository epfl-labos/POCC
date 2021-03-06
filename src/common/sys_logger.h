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


#ifndef SCC_COMMON_SYS_EVENT_LOGGER_H
#define SCC_COMMON_SYS_EVENT_LOGGER_H

#include <string>
#include <thread>
#include <mutex>
#include <boost/format.hpp>

namespace scc {

#define SLOG(x) (SysLogger::Instance()->Log(x))
#define SDEBUG(x) (SysLogger::Instance()->Debug(x))

class SysLogger {
public:
  ~SysLogger();
  static SysLogger* Instance();
  void Log(std::string msg);
  void Debug(std::string msg);
private:
  SysLogger();
  FILE* _logStream;
  FILE* _debugStream;
  std::mutex _logStreamMutex;
  std::mutex _debugStreamMutex;
};

} // namespace scc

#endif
