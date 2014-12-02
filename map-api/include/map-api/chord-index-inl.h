#ifndef MAP_API_CHORD_INDEX_INL_H_
#define MAP_API_CHORD_INDEX_INL_H_
#include <vector>

namespace map_api {
template <typename DataType>
ChordIndex::Key ChordIndex::hash(const DataType& data) {
  // TODO(tcies) better method?
  Poco::MD5Engine md5;
  Poco::DigestOutputStream digest_stream(md5);
  digest_stream << data;
  digest_stream.flush();
  const Poco::DigestEngine::Digest& digest = md5.digest();
  constexpr bool digest_still_uchar_vec = std::is_same<
      Poco::DigestEngine::Digest, std::vector<unsigned char> >::value;
  static_assert(digest_still_uchar_vec,
                "Underlying type of Digest changed since Poco 1.3.6");
  union KeyUnion {
    Key key;
    unsigned char bytes[sizeof(Key)];
  };

  static_assert(sizeof(Key) == sizeof(KeyUnion), "Bad union size");
  KeyUnion return_value;
  memcpy(return_value.bytes, &digest[0], sizeof(KeyUnion));

  return return_value.key;
}
}  // namespace map_api

#endif  // MAP_API_CHORD_INDEX_INL_H_
