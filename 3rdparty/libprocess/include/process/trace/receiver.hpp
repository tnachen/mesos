#ifndef __PROCESS_RECEIVER_HPP__
#define __PROCESS_RECEIVER_HPP__

#include <process/dispatch.hpp>
#include <process/future.hpp>
#include <process/message.hpp>
#include <process/pid.hpp>
#include <process/process.hpp>

#include <process/trace/span.hpp>

namespace process {
namespace trace {

enum Stage
{
  MESSAGE_INBOUND_QUEUED,
  MESSAGE_OUTBOUND_QUEUED
};

namespace internal {

/**
 * Receiver records all span messages received from this libprocess.
 */
class ReceiverProcess : public process::Process<ReceiverProcess>
{
public:
  static ReceiverProcess* instance();

  virtual ~ReceiverProcess() {}

  virtual void receive(
    const Span& span,
    const std::string& name,
    const Time& time,
    const UPID& from,
    const UPID& to,
    const Stage& stage,
    const std::string& component) = 0;

protected:
  ReceiverProcess() {};
};


} // namespace internal {


inline process::Future<Nothing> record(
    const Message* message,
    const Stage& stage,
    const Option<std::string>& component = None())
{
  if (message->span.isSome()) {
    dispatch(
        internal::ReceiverProcess::instance(),
        &internal::ReceiverProcess::receive,
        message->span.get(),
        message->name,
        Clock::now(),
        message->from,
        message->to,
        stage,
        component.getOrElse(message->component));
  }

  return Nothing();
}


inline process::Future<Nothing> record(
    const Event* event,
    const Stage& stage,
    const Option<std::string>& component = None())
{
  struct TraceEventVisitor : EventVisitor
  {
    explicit TraceEventVisitor(
        const Stage& _stage,
        const Option<std::string>& _component)
      : stage(_stage),
        component(_component) {}

    virtual void visit(const MessageEvent& event)
    {
      record(event.message, stage, component);
    }

    Stage stage;
    Option<std::string> component;
  } visitor(stage, component);

  event->visit(&visitor);

  return Nothing();
}


} // namespace trace {
} // namespace process {

#endif // __PROCESS_RECEIVER_HPP__
