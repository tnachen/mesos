#ifndef __PROCESS_SPAN_HPP__
#define __PROCESS_SPAN_HPP__

#include <utility>

#include <stout/option.hpp>
#include <stout/uuid.hpp>

namespace process {
namespace trace {

struct Span
{
  Span()
    : id(UUID::random()),
      traceId(UUID::random()) {}

  Span(
      const UUID& _traceId,
      const Option<UUID> _parent = None(),
      const Option<std::string>& _tags = None())
    : id(UUID::random()),
      parent(_parent),
      traceId(_traceId),
      tags(_tags) {}

  UUID id;
  Option<UUID> parent;

  UUID traceId;

  // A list of key value pairs delimited with ';'.
  // (e.g: framework=foo;task=bar).
  Option<std::string> tags;
};


} // namespace trace {
} // namespace process {

#endif // __PROCESS_SPAN_HPP__
