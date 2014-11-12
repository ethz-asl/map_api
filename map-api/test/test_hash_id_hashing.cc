#include <functional>
#include <memory>
#include <unordered_map>

#include <sm/hash_id.hpp>
#include <timing/timer.h>

#include "map-api/unique-id.h"
#include "map-api/test/testing_entrypoint.h"

#define DEFINE_ID_HASH_SIZET(TypeName)                          \
  namespace std {                                               \
  template <>                                                   \
  struct hash<TypeName> {                                       \
    typedef TypeName argument_type;                             \
    typedef std::size_t value_type;                             \
    value_type operator()(const argument_type& hash_id) const { \
      return hash_id.hashToSizeT();                             \
    }                                                           \
  };                                                            \
  }                                                             \
  extern void defineId##__FILE__##__LINE__(void)

#define DEFINE_ID_HASH_STRING(TypeName)                         \
  namespace std {                                               \
  template <>                                                   \
  struct hash<TypeName> {                                       \
    typedef TypeName argument_type;                             \
    typedef std::size_t value_type;                             \
    value_type operator()(const argument_type& hash_id) const { \
      return std::hash<std::string>()(hash_id.hexString());     \
    }                                                           \
  };                                                            \
  }                                                             \
  extern void defineId##__FILE__##__LINE__(void)

typedef std::shared_ptr<double> Data;

UNIQUE_ID_DEFINE_ID(TestIdStringHash);
DEFINE_ID_HASH_STRING(TestIdStringHash);

UNIQUE_ID_DEFINE_ID(TestIdSizeTHash);
DEFINE_ID_HASH_SIZET(TestIdSizeTHash);

TEST(MultiagentMappingCommon, HashIdHashingLoadDistribution) {
  std::unordered_map<TestIdStringHash, Data> map_string_hashes;
  std::unordered_map<TestIdSizeTHash, Data> map_sizet_hashes;
  constexpr int kNumTrials = 100000;
  Data item;
  for (int i = 0; i < kNumTrials; ++i) {
    TestIdStringHash id;
    map_api::generateId(&id);
    map_string_hashes[id] = item;
  }

  for (int i = 0; i < kNumTrials; ++i) {
    TestIdSizeTHash id;
    map_api::generateId(&id);
    map_sizet_hashes[id] = item;
  }

  std::cout << "String hasher" << std::endl;
  std::cout << "size = " << map_string_hashes.size() << std::endl;
  std::cout << "bucket_count = " << map_string_hashes.bucket_count()
            << std::endl;
  std::cout << "load_factor = " << map_string_hashes.load_factor() << std::endl;
  std::cout << "max_load_factor = " << map_string_hashes.max_load_factor()
            << std::endl;

  std::cout << std::endl;

  std::cout << "Size T hasher" << std::endl;
  std::cout << "size = " << map_sizet_hashes.size() << std::endl;
  std::cout << "bucket_count = " << map_sizet_hashes.bucket_count()
            << std::endl;
  std::cout << "load_factor = " << map_sizet_hashes.load_factor() << std::endl;
  std::cout << "max_load_factor = " << map_sizet_hashes.max_load_factor()
            << std::endl;
}

TEST(MultiagentMappingCommon, HashIdHashingSpeed) {
  std::unordered_map<TestIdStringHash, Data> map_string_hashes;
  std::unordered_map<TestIdSizeTHash, Data> map_sizet_hashes;
  constexpr int kNumTrials = 100000;
  Data item;
  size_t avoid_optimization1 = 0;
  timing::Timer timer_string_hash("String hash");
  std::hash<TestIdStringHash> hasher;
  for (int i = 0; i < kNumTrials; ++i) {
    TestIdStringHash id;
    map_api::generateId(&id);
    avoid_optimization1 ^= hasher(id);
  }
  timer_string_hash.Stop();

  size_t avoid_optimization2 = 0;
  timing::Timer timer_sizet_hash("SizeT hash");
  for (int i = 0; i < kNumTrials; ++i) {
    TestIdSizeTHash id;
    map_api::generateId(&id);
    avoid_optimization2 ^= id.hashToSizeT();
  }
  timer_sizet_hash.Stop();

  std::cout << avoid_optimization1 << avoid_optimization2 << std::endl;

  timing::Timing::Print(std::cout);
}

MAP_API_UNITTEST_ENTRYPOINT
