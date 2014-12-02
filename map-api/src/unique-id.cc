#include "map-api/internal/unique-id.h"
#include <atomic>
#include <chrono>

#include <Poco/DigestStream.h>
#include <Poco/MD5Engine.h>

#include "map-api/hub.h"

namespace map_api {
namespace internal {
std::string generateUniqueHexString() {
  static std::atomic<int> counter;
  ++counter;
  Poco::MD5Engine md5;
  Poco::DigestOutputStream digest_stream(md5);
  digest_stream << counter;
  digest_stream
      << std::chrono::high_resolution_clock::now().time_since_epoch().count();
  digest_stream << Hub::instance().ownAddress();
  digest_stream.flush();
  const Poco::DigestEngine::Digest& digest = md5.digest();
  return Poco::DigestEngine::digestToHex(digest);
}
}  // namespace internal
}  // namespace map_api
