#include <stdlib.h>

#include <process/once.hpp>
#include <process/pid.hpp>
#include <process/process.hpp>

#include <process/trace/receiver.hpp>

#include <stout/json.hpp>
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
      const Stage& stage,
      const string& component);

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
    const Stage& stage,
    const string& component)
{
  if (fd.isNone()) {
    string tracesPath =
      os::getenv("LIBPROCESS_TRACE_FILE").getOrElse("/tmp/traces");

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

  JSON::Object object;

  object.values["trace_id"]  = stringify(span.traceId);
  object.values["msg_name"] = name;
  object.values["span_id"] = stringify(span.id);

  if (span.parent.isSome()) {
    object.values["span_parent"] = stringify(span.parent.get());
  } else {
    object.values["span_parent"] = 0;
  }

  object.values["from"] = stringify(from);
  object.values["to"] = stringify(to);

  object.values["duration"] = time.duration().ns();
  object.values["component"] = component;

  std::ostringstream out;

  switch (stage) {
    case MESSAGE_INBOUND_QUEUED:
      object.values["message_direction"] = "message_inbound_queued";
      break;
    case MESSAGE_OUTBOUND_QUEUED:
      object.values["message_direction"] = "message_outbound_queued";
      break;
  }

  out << object;
  out << "\r\n";
  os::write(fd.get(), out.str());
}


} // namespace internal {
} // namespace trace {
} // namespace process {
