#include <stdlib.h>

#include <process/once.hpp>
#include <process/pid.hpp>
#include <process/process.hpp>

#include <process/trace/receiver.hpp>

namespace process {
namespace trace {
namespace internal {

class LocalReceiverProcess : public ReceiverProcess
{
public:
  void receive(const Span& span, const UPID& from, const UPID& to);

private:
  Option<int> fd;
};


ReceiverProcess* ReceiverProcess::instance() {
  static ReceiverProcess* receiver = NULL;
  static Once* initialized = new Once();

  if (!initialized->once()) {
    // TODO: Create receiver with modules.
    receiver = new LocalReceiverProcess();
    spawn(receiver);
    initialized->done();
  }

  return receiver;
}


void LocalReceiverProcess::receive(
    const Span& span,
    const UPID& from,
    const UPID& to)
{
  if (fd.isNone()) {
    Try<int> open = os::open(
        os::hasenv("LIBPROCESS_TRACE_FILE")
        ? os::getenv("LIBPROCESS_TRACE_FILE")
        : "/tmp/mesos/traces",
        O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC,
        S_IRUSR | S_IWUSR | S_IRGRP | S_IRWXO);

    if (open.isError()) {
      ABORT("Unable to open file for writing traces: " + open.error());
    }

    fd = open.get();
  }

  std::ostringstream out;
  out << span.traceId << ","
      << span.id << ",";

  if (span.parent.isSome()) {
    out << span.parent.get();
  }

  out << ",";

  out << span.start << ","
      << span.end.get();

  os::write(fd.get(), out.str());
}


} // namespace internal {
} // namespace trace {
} // namespace process {
