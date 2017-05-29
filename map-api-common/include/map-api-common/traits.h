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
