// Copyright (C) 2014-2017 Titus Cieslewski, ASL, ETH Zurich, Switzerland
// You can contact the author at <titus at ifi dot uzh dot ch>
// Copyright (C) 2014-2015 Simon Lynen, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014-2015, Marcin Dymczyk, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014, Stéphane Magnenat, ASL, ETH Zurich, Switzerland
//
// This file is part of Map API.
//
// Map API is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Map API is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Map API.  If not, see <http://www.gnu.org/licenses/>.

#ifndef DMAP_COMMON_BACKTRACE_H_
#define DMAP_COMMON_BACKTRACE_H_

#include <cxxabi.h>
#include <execinfo.h>
#include <memory>
#include <sstream>  // NOLINT
#include <string>

namespace map_api_common {

// Adapted from http://panthema.net/2008/0901-stacktrace-demangled/ .
inline std::string backtrace() {
  constexpr size_t kBacktraceLimit = 64;
  void* array[kBacktraceLimit];
  size_t size;

  size = ::backtrace(array, kBacktraceLimit);
  // Causes
  // http://valgrind.org/docs/manual/mc-manual.html#opt.show-mismatched-frees ,
  // but that is ok.
  std::unique_ptr<char * []> symbollist(backtrace_symbols(array, size));

  if (size == 0u) {
    return "  Backtrace empty, possibly corrupt!";
  }

  std::stringstream result;
  for (size_t i = 1u; i < size; i++) {
    char* begin_name = 0, *begin_offset = 0, *end_offset = 0;

    // Find parentheses and +address offset surrounding the mangled name:
    // ./module(function+0x15c) [0x8048a6d]
    for (char* p = symbollist[i]; *p; ++p) {
      if (*p == '(') {
        begin_name = p;
      } else if (*p == '+') {
        begin_offset = p;
      } else if (*p == ')' && begin_offset) {
        end_offset = p;
        break;
      }
    }

    if (begin_name && begin_offset && end_offset && begin_name < begin_offset) {
      *begin_name++ = '\0';
      *begin_offset++ = '\0';
      *end_offset = '\0';

      // Mangled name is now in [begin_name, begin_offset) and caller
      // offset in [begin_offset, end_offset).
      int status;
      // Causes
      // http://valgrind.org/docs/manual/mc-manual.html#opt.show-mismatched-frees
      // but that is ok.
      std::unique_ptr<char> demangled(
          abi::__cxa_demangle(begin_name, nullptr, nullptr, &status));
      if (status == 0) {
        result << "  " << symbollist[i] << " : " << demangled.get() << "+"
               << begin_offset << "\n";
      } else {
        // Demangling failed. Output function name as a C function with
        // no arguments.
        result << "  " << symbollist[i] << " : " << begin_name << "+"
               << begin_offset << "\n";
      }
    } else {
      // Couldn't parse the line? Print the whole line.
      result << "  " << symbollist[i] << "\n";
    }
  }

  return result.str();
}

}  // namespace map_api_common

#endif  // DMAP_COMMON_BACKTRACE_H_
