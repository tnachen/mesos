#ifndef __PROCESS_SPAN_HPP__
#define __PROCESS_SPAN_HPP__

#include <utility>

#include <stout/option.hpp>
#include <stout/uuid.hpp>

namespace process {
namespace trace {

/**
 * A Span is a message that belongs to a trace.
 * The Span concept is from the Google Dapper paper.
 * A trace consists of multiple spans:
 *                              Span D
 * Span A -> Span B -> Span C /
 *                            \ Span E
 */
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
