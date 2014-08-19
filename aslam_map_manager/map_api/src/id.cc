#include "map-api/id.h"

#include <atomic>
#include <chrono>

#include <glog/logging.h>

#include <Poco/DigestStream.h>
#include <Poco/MD5Engine.h>

#include "map-api/hub.h"

namespace map_api {

bool Id::fromHashId(const sm::HashId& id) {
  return fromHexString(id.hexString());
}

Id Id::generate(){
  static std::atomic<int> counter;
  ++counter;
  Poco::MD5Engine md5;
  Poco::DigestOutputStream digest_stream(md5);
  digest_stream << counter;
  digest_stream <<
      std::chrono::high_resolution_clock::now().time_since_epoch().count();
  digest_stream << Hub::instance().ownAddress();
  digest_stream.flush();
  const Poco::DigestEngine::Digest& digest = md5.digest();
  Id generated;
  generated.fromHexString(Poco::DigestEngine::digestToHex(digest));
  return generated;
}

} // namespace map_api
