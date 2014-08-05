#ifndef ID_H_
#define ID_H_

#include <sm/hash_id.hpp>

namespace map_api{
class Id : public sm::HashId {
 public:
  Id() = default;
  bool fromHashId(const sm::HashId& id);
  static Id generate();
 private:
  using sm::HashId::random;
};
} // namespace map_api

namespace std{

inline ostream& operator<<(ostream& out, const map_api::Id& hash) {
  out << "Id(" << hash.hexString() << ")";
  return out;
}

template<>
struct hash<map_api::Id>{
  std::size_t operator()(const map_api::Id& hashId) const {
    return std::hash<std::string>()(hashId.hexString());
  }
};
} // namespace std

#endif /* ID_H_ */
