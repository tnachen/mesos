/**
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License
*/

#ifndef __PROCESS_PARALLEL_HPP__
#define __PROCESS_PARALLEL_HPP__

#include <list>

#include <process/check.hpp>
#include <process/defer.hpp>
#include <process/future.hpp>
#include <process/owned.hpp>
#include <process/process.hpp>

#include <stout/lambda.hpp>

// TODO(bmahler): Move these into a futures.hpp header to group Future
// related utilities.

namespace process {

// Runs a specified number of producers in parallel and returns the
// aggregated results.
template <typename T>
Future<std::list<T>> parallel(
    const std::list<lambda::function<Future<T>()>>& producers,
    size_t limit);


namespace internal {

template <typename T>
class ParallelProcess : public Process<ParallelProcess<T>>
{
public:
  ParallelProcess(
      const std::list<lambda::function<Future<T>()>>& _producers,
      Promise<std::list<T>>* _promise,
      size_t _limit)
    : producers(_producers),
      promise(_promise),
      limit(_limit),
      next(0),
      ready(0)
  {
    iterator = producers.begin();
  }

  virtual ~ParallelProcess()
  {
    delete promise;
  }

  virtual void initialize()
  {
    promise->future().onDiscard(defer(this, &ParallelProcess::discarded));

    for (int i = 0; (limit == 0 || i < limit) && i < producers.size(); ++i) {
      (*++iterator)().onAny(defer(this, &ParallelProcess::waited, lambda::_1));
    }
  }

private:
  void discarded()
  {
    promise->discard();
    terminate(this);
  }

  void waited(Future<T> future)
  {
    if (future.isFailed()) {
      promise->fail("Parallel failed: " + future.failure());
      terminate(this);
    } else if (future.isDiscarded()) {
      promise->fail("Parallel failed: future discarded");
      terminate(this);
    } else {
      CHECK_READY(future);
      ready += 1;
      results.push_back(future.get());
      if (ready == producers.size()) {
        promise->set(results);
        terminate(this);
      } else if (iterator != producers.end()) {
        async(*++iterator).onAny(defer(this, &ParallelProcess::waited, lambda::_1));
      }
    }
  }

  const std::list<lambda::function<Future<T>()>> producers;
  typename std::list<lambda::function<Future<T>()>>::const_iterator iterator;
  std::list<T> results;
  Promise<std::list<T>>* promise;
  size_t limit;
  size_t next;
  size_t ready;
};


} // namespace internal {


template <typename T>
inline Future<std::list<T>> parallel(
    const std::list<lambda::function<Future<T>()>>& producers,
    size_t limit)
{
  if (producers.empty()) {
    return std::list<T>();
  }

  Promise<std::list<T>>* promise = new Promise<std::list<T>>();
  Future<std::list<T>> future = promise->future();
  spawn(new internal::ParallelProcess<T>(producers, promise, limit), true);
  return future;
}

} // namespace process {

#endif // __PROCESS_PARALLEL_HPP__
