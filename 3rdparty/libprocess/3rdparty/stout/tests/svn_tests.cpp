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

#include <gtest/gtest.h>

#include <cstdlib> // For rand().
#include <string>
#include <vector>

#include <stout/bytes.hpp>
#include <stout/gtest.hpp>
#include <stout/svn.hpp>
#include <stout/stopwatch.hpp>

using std::string;
using std::vector;

struct Size {
  int diff;
  int orig;
  Duration duration;
};


const int STEP = 16 * 1024;


static void benchDiff(int size, double diffPercent, bool silent)
{
  vector<Size*> all;
  for(int i = 0; i < size / STEP; ++i) {
    Size* s = new Size();
    all.push_back(s);
  }
  int tries = 50;
  if (!silent) {
    std::cout << "Benchmarking with max size: " << Bytes(size)
              << " and percent modified: " << stringify(diffPercent)
              << ", Averaging over " << stringify(tries) << " trials"
              << std::endl;
  }


  int current = 1;
  while(current <= tries) {
    int count = 0;
    int currentSize = STEP;
    while(currentSize <= size) {
      string source;

      while (Bytes(source.size()) < Bytes(currentSize)) {
        source += (char) rand() % 256;
      }

      string target = source;
      int maxOffset = currentSize / 2 + currentSize * diffPercent;

      for(int i = currentSize / 2; i < maxOffset && i < currentSize; ++i) {
        target[i] = (char) rand() % 256;
      }

      Stopwatch stopwatch;
      stopwatch.start();

      Try<svn::Diff> diff = svn::diff(source, target);

      stopwatch.stop();

      ASSERT_SOME(diff);


      Size* s = all[count];
      s->diff = diff.get().data.size();
      s->orig = currentSize;
      if (current == 1) {
        s->duration = stopwatch.elapsed();
      } else {
        s->duration += stopwatch.elapsed();
      }

      currentSize += STEP;
      count++;
    }
    current++;
  }

  if (!silent) {
    foreach(Size* size, all) {
      int cost =
        (size->duration.ns() / tries) + (size->diff * 2.119) +
        (size->diff * 0.5);
      std::cout << cost << std::endl;
      delete size;
    }
  }
}


TEST(SVN, DiffPatch)
{
  string source;

  while (Bytes(source.size()) < Megabytes(1)) {
    source += (char) rand() % 256;
  }

  // Make the target string have 512 different bytes in the middle.
  string target = source;

  for (size_t index = 0; index < 512; index++) {
    target[1024 + index] = (char) rand() % 256;
  }

  ASSERT_NE(source, target);

  Try<svn::Diff> diff = svn::diff(source, target);

  ASSERT_SOME(diff);

  Try<string> result = svn::patch(source, diff.get());

  ASSERT_SOME_EQ(target, result);
  ASSERT_SOME_NE(source, result);
}


TEST(SVN, BENCHMARK_DiffPerf)
{
  benchDiff(1024 * 8, 0.1, true);

  for (double i = 0.1; i <= 0.3; i += 0.05) {
    benchDiff(1024 * 1024, i, false);
  }
}
