/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef __STOUT_SVN_HPP__
#define __STOUT_SVN_HPP__

#include <apr_pools.h>

#include <stdlib.h>

#include <svn_delta.h>
#include <svn_error.h>
#include <svn_pools.h>

#include <string>

#include <stout/try.hpp>

namespace svn {

struct Diff
{
  Diff(const std::string& data) : data(data) {}

  std::string data;
};


namespace internal {

// Helper for initializing and terminating the APR (Apache Portable
// Runtime) library. Note that according to the documentation there
// can be as many calls to 'apr_terminate' as 'apr_finalize' so it
// should be legal to use this multiple times.
struct APR
{
  APR()
  {
    apr_initialize();
    atexit(apr_terminate);
  }
};

} // namespace internal {


inline Try<Diff> diff(const std::string& from, const std::string& to)
{
  // Initialize the Apache Portable Runtime library which also makes
  // sure the library gets terminated at program exit.
  static internal::APR apr;

  apr_pool_t* pool = svn_pool_create(NULL);

  // First we need to produce a text delta stream by diffing 'source'
  // against 'target'.
  svn_string_t source;
  source.data = from.data();
  source.len = from.length();

  svn_string_t target;
  target.data = to.data();
  target.len = to.length();

  svn_txdelta_stream_t* delta;
  svn_txdelta2(
      &delta,
      svn_stream_from_string(&source, pool),
      svn_stream_from_string(&target, pool),
      false,
      pool);

  // Now we want to convert this text delta stream into an svndiff
  // format based diff. Setup the handler that will consume the text
  // delta and produce the svndiff.
  svn_txdelta_window_handler_t handler;
  void* baton = NULL;
  svn_stringbuf_t* diff = svn_stringbuf_create_ensure(1024, pool);

  svn_txdelta_to_svndiff3(
      &handler,
      &baton,
      svn_stream_from_stringbuf(diff, pool),
      0,
      SVN_DELTA_COMPRESSION_LEVEL_DEFAULT,
      pool);

  // Now feed the text delta to the handler.
  svn_error_t* error = svn_txdelta_send_txstream(delta, handler, baton, pool);

  if (error != NULL) {
    char buffer[1024];
    std::string message(svn_err_best_message(error, buffer, 1024));
    svn_pool_destroy(pool);
    return Error(message);
  }

  Diff d(std::string(diff->data, diff->len));

  svn_pool_destroy(pool);

  return d;
}


inline Try<std::string> patch(const std::string& s, const Diff& diff)
{
  // Initialize the Apache Portable Runtime library which also makes
  // sure the library gets terminated at program exit.
  static internal::APR apr;

  apr_pool_t* pool = svn_pool_create(NULL);

  // We want to apply the svndiff format diff to the source trying to
  // produce a result. First setup a handler for applying a text delta
  // to the source stream.
  svn_string_t source;
  source.data = s.data();
  source.len = s.length();

  svn_txdelta_window_handler_t handler;
  void* baton = NULL;

  svn_stringbuf_t* patched = svn_stringbuf_create_ensure(s.length(), pool);

  svn_txdelta_apply(
      svn_stream_from_string(&source, pool),
      svn_stream_from_stringbuf(patched, pool),
      NULL,
      NULL,
      pool,
      &handler,
      &baton);

  // Setup a stream that converts an svndiff format diff to a text
  // delta, so that we can use our handler to patch the source string.
  svn_stream_t* stream = svn_txdelta_parse_svndiff(
      handler,
      baton,
      TRUE,
      pool);

  // Now feed the diff into the stream to compute the patched result.
  const char* data = diff.data.data();
  apr_size_t length = diff.data.length();

  svn_error_t* error = svn_stream_write(stream, data, &length);

  if (error != NULL) {
    char buffer[1024];
    std::string message(svn_err_best_message(error, buffer, 1024));
    svn_pool_destroy(pool);
    return Error(message);
  }

  std::string result(patched->data, patched->len);

  svn_pool_destroy(pool);

  return result;
}

} // namespace svn {

#endif // __STOUT_SVN_HPP__
