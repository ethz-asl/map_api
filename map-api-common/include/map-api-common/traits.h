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

#ifndef DMAP_COMMON_TRAITS_H_
#define DMAP_COMMON_TRAITS_H_
#include <memory>

namespace map_api_common {

template <typename T>
struct IsPointerType {
  typedef T type;
  typedef const T& const_ref_type;
  enum {
    value = false
  };
};
template <typename T>
struct IsPointerType<T&> {
  typedef T type;
  typedef const T& const_ref_type;
  enum {
    value = false
  };
};
template <typename T>
struct IsPointerType<const T&> {
  typedef T type;
  typedef const T& const_ref_type;
  enum {
    value = false
  };
};
template <typename T>
struct IsPointerType<T*> {
  typedef T type;
  typedef const T& const_ref_type;
  enum {
    value = true
  };
};
template <typename T>
struct IsPointerType<const T*> {
  typedef T type;
  typedef const T& const_ref_type;
  enum {
    value = true
  };
};
template <typename T>
struct IsPointerType<std::shared_ptr<T> > {
  typedef T type;
  typedef const std::shared_ptr<const T> const_ref_type;
  enum {
    value = true
  };
};
template <typename T>
struct IsPointerType<const std::shared_ptr<T> > {
  typedef T type;
  typedef const std::shared_ptr<const T> const_ref_type;
  enum {
    value = true
  };
};
template <typename T>
struct IsPointerType<std::shared_ptr<const T> > {
  typedef T type;
  typedef const std::shared_ptr<const T> const_ref_type;
  enum {
    value = true
  };
};
template <typename T>
struct IsPointerType<const std::shared_ptr<const T> > {
  typedef T type;
  typedef const std::shared_ptr<const T> const_ref_type;
  enum {
    value = true
  };
};

template <class ValueType>
typename std::enable_if<IsPointerType<ValueType>::value == false, int64_t>::type
ExtractTimeStamp(const ValueType& value) {
  return value.timestamp;
}

template <class ValueType>
typename std::enable_if<IsPointerType<ValueType>::value, int64_t>::type
ExtractTimeStamp(const ValueType& value) {
  return value->timestamp;
}
}  // namespace #ifndef MULTIAGENT_MAPPING_COMMON_TRAITS_H_
#define MULTIAGENT_MAPPING_COMMON_TRAITS_H_
#include <memory>

namespace common {

template <typename T>
struct IsPointerType {
  typedef T type;
  typedef const T& const_ref_type;
  enum {
    value = false
  };
};
template <typename T>
struct IsPointerType<T&> {
  typedef T type;
  typedef const T& const_ref_type;
  enum {
    value = false
  };
};
template <typename T>
struct IsPointerType<const T&> {
  typedef T type;
  typedef const T& const_ref_type;
  enum {
    value = false
  };
};
template <typename T>
struct IsPointerType<T*> {
  typedef T type;
  typedef const T& const_ref_type;
  enum {
    value = true
  };
};
template <typename T>
struct IsPointerType<const T*> {
  typedef T type;
  typedef const T& const_ref_type;
  enum {
    value = true
  };
};
template <typename T>
struct IsPointerType<std::shared_ptr<T> > {
  typedef T type;
  typedef const std::shared_ptr<const T> const_ref_type;
  enum {
    value = true
  };
};
template <typename T>
struct IsPointerType<const std::shared_ptr<T> > {
  typedef T type;
  typedef const std::shared_ptr<const T> const_ref_type;
  enum {
    value = true
  };
};
template <typename T>
struct IsPointerType<std::shared_ptr<const T> > {
  typedef T type;
  typedef const std::shared_ptr<const T> const_ref_type;
  enum {
    value = true
  };
};
template <typename T>
struct IsPointerType<const std::shared_ptr<const T> > {
  typedef T type;
  typedef const std::shared_ptr<const T> const_ref_type;
  enum {
    value = true
  };
};

template <class ValueType>
typename std::enable_if<IsPointerType<ValueType>::value == false, int64_t>::type
ExtractTimeStamp(const ValueType& value) {
  return value.timestamp;
}

template <class ValueType>
typename std::enable_if<IsPointerType<ValueType>::value, int64_t>::type
ExtractTimeStamp(const ValueType& value) {
  return value->timestamp;
}
}  // namespace map_api_common

#endif  // DMAP_COMMON_TRAITS_H_
