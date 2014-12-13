#ifndef __PROCESS_SPAN_HPP__
#define __PROCESS_SPAN_HPP__

#include <stout/option.hpp>
#include <stout/uuid.hpp>

namespace process {
namespace trace {

struct Span
{
  Span() : id(UUID::random()), traceId(UUID::random()) {}

  Span(const UUID& _traceId, Option<UUID> _parent = None())
    : id(UUID::random()), parent(_parent), traceId(_traceId) {}

  UUID id;
  Option<UUID> parent;

  UUID traceId;
};

} // namespace trace {
} // namespace process {

#endif // __PROCESS_SPAN_HPP__
