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

#ifndef DMAP_COMMON_ALIGNED_ALLOCATION_H_
#define DMAP_COMMON_ALIGNED_ALLOCATION_H_

#include <functional>
#include <map>
#include <memory>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <Eigen/Core>
#include <Eigen/StdVector>

#include "map-api-common/eigen-hash.h"

template<template<typename, typename> class Container, typename Type>
struct Aligned {
  typedef Container<Type, Eigen::aligned_allocator<Type> > type;
};

template <typename KeyType, typename ValueType>
struct AlignedMap {
  typedef std::map<
      KeyType, ValueType, std::less<KeyType>,
      Eigen::aligned_allocator<std::pair<const KeyType, ValueType> > > type;
};

template<typename KeyType, typename ValueType>
struct AlignedUnorderedMap {
  typedef std::unordered_map<KeyType, ValueType,
      std::hash<KeyType>, std::equal_to<KeyType>,
      Eigen::aligned_allocator<std::pair<const KeyType, ValueType> > > type;
};

template <typename KeyType, typename ValueType>
struct AlignedUnorderedMultimap {
  typedef std::unordered_multimap<
      KeyType, ValueType, std::hash<KeyType>, std::equal_to<KeyType>,
      Eigen::aligned_allocator<std::pair<const KeyType, ValueType> > > type;
};

template <typename Type>
struct AlignedUnorderedSet {
  typedef std::unordered_set<Type, std::hash<Type>, std::equal_to<Type>,
                             Eigen::aligned_allocator<Type>> type;
};

template<typename Type, typename ... Arguments>
inline std::shared_ptr<Type> aligned_shared(Arguments&&... arguments) {
  return std::shared_ptr<Type>(new Type(std::forward<Arguments>(arguments)...));
}

namespace internal {
template <typename Type>
struct aligned_delete {
  constexpr aligned_delete() noexcept = default;

  template <typename TypeUp,
            typename = typename std::enable_if<
                std::is_convertible<TypeUp*, Type*>::value>::type>
  aligned_delete(const aligned_delete<TypeUp>&) noexcept {}

  void operator()(Type* ptr) const {
    static_assert(sizeof(Type) > 0, "Can't delete pointer to incomplete type!");
    typedef typename std::remove_const<Type>::type TypeNonConst;
    Eigen::aligned_allocator<TypeNonConst> allocator;
    allocator.deallocate(ptr, 1u /*num*/);
  }
};
}  // namespace internal

template <typename Type>
struct AlignedUniquePtr {
  typedef typename std::remove_const<Type>::type TypeNonConst;
  typedef std::unique_ptr<Type, internal::aligned_delete<TypeNonConst>> type;
};

template <typename Type, typename... Arguments>
inline typename AlignedUniquePtr<Type>::type aligned_unique(
    Arguments&&... arguments) {
  typedef typename std::remove_const<Type>::type TypeNonConst;
  Eigen::aligned_allocator<TypeNonConst> allocator;
  TypeNonConst* obj = ::new (allocator.allocate(1u))  // NOLINT
      Type(std::forward<Arguments>(arguments)...);
  return std::move(typename AlignedUniquePtr<Type>::type(obj));
}

#endif  // DMAP_COMMON_ALIGNED_ALLOCATION_H_
