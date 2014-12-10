#ifndef __PROCESS_RECEIVER_HPP__
#define __PROCESS_RECEIVER_HPP__

#include <process/dispatch.hpp>
#include <process/message.hpp>
#include <process/pid.hpp>

#include <process/trace/span.hpp>

namespace process {
namespace trace {

namespace internal {

class ReceiverProcess : public process::Process<ReceiverProcess>
{
public:
  static ReceiverProcess* instance();

  virtual ~ReceiverProcess() {}

  virtual void receive(const Span& span, const UPID& from, const UPID& to) = 0;

protected:
  ReceiverProcess() {}

private:
  // Non-copyable, non-assignable.
  ReceiverProcess(const ReceiverProcess&);
  ReceiverProcess& operator = (const ReceiverProcess&);
};

} // namespace internal {


Future<Nothing> receive(const Message* message)
{
  if (message->span.isSome()) {
    dispatch(
        internal::ReceiverProcess::instance(),
        &internal::ReceiverProcess::receive,
        message->span.get(),
        message->from,
        message->to);
  }

  return Nothing();
}

} // namespace trace {
} // namespace process {

#endif // __PROCESS_RECEIVER_HPP__
