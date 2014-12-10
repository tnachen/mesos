#ifndef __PROCESS_SPAN_HPP__
#define __PROCESS_SPAN_HPP__

#include <stout/option.hpp>
#include <stout/uuid.hpp>

namespace process {
namespace trace {

struct Span
{
  Span(const UUID& _traceId, Option<UUID> _parent = None())
    : id(UUID::random()), traceId(_traceId), parent(_parent) {}

  UUID id;
  Option<UUID> parent;
  UUID traceId;

  Time start;
  Option<Time> end;
};

} // namespace trace {
} // namespace process {

#endif // __PROCESS_SPAN_HPP__
