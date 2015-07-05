#include <stdlib.h>

#include <process/once.hpp>
#include <process/pid.hpp>
#include <process/process.hpp>

#include <process/trace/receiver.hpp>

#include <stout/os.hpp>

namespace process {
namespace trace {
namespace internal {

using std::string;

class LocalReceiverProcess : public ReceiverProcess
{
public:
  LocalReceiverProcess(): ProcessBase("", true) {};

  virtual ~LocalReceiverProcess() {};

  virtual void receive(
      const Span& span,
      const string& name,
      const Time& time,
      const UPID& from,
      const UPID& to,
      const Stage& stage);

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
    const string& name,
    const Time& time,
    const UPID& from,
    const UPID& to,
    const Stage& stage)
{
  if (fd.isNone()) {
    string tracesPath = os::getenv("LIBPROCESS_TRACE_FILE").get("/tmp/traces");

    Try<int> open = os::open(
        tracesPath,
        O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC,
        S_IRUSR | S_IWUSR | S_IRGRP | S_IRWXO);

    if (open.isError()) {
      ABORT("Unable to open file " + tracesPath + " for writing traces: "
            + open.error());
    }

    fd = open.get();
  }

  // Writing out a CSV of the trace.
  // Schema: trace_id, name, span_id, span_parent_id, from, to,
  //         current_time, stage.
  std::ostringstream out;
  out << span.traceId << ","
      << name << ","
      << span.id << ",";

  if (span.parent.isSome()) {
    out << span.parent.get();
  }

  out << ",";

  out << from << "," << to << ",";

  out << time.duration().ns() << ",";

  switch (stage) {
    case MESSAGE_INBOUND_QUEUED:
      out << "message_inbound_queued";
      break;
    case MESSAGE_OUTBOUND_QUEUED:
      out << "message_outbound_queued";
      break;
  }

  out << "\r\n";

  os::write(fd.get(), out.str());
}


} // namespace internal {
} // namespace trace {
} // namespace process {
