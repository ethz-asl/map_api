#include "map-api/discovery.h"

#include <fstream>
#include <sstream>
#include <string>

#include <sys/file.h> // linux-specific

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "map-api/map-api-hub.h"

namespace map_api {

void Discovery::announce() const {
  append(MapApiHub::instance().ownAddress());
}

int Discovery::getPeers(std::vector<PeerId>* peers) const {
  CHECK_NOTNULL(peers);
  std::string file_contents;
  getFileContents(&file_contents);
  std::istringstream discovery_stream(file_contents);
  std::string address;
  while (discovery_stream >> address) {
    if (address == MapApiHub::instance().ownAddress() || address == "") {
      continue;
    }
    peers->push_back(PeerId(address));
  }
  return peers->size();
}

void Discovery::leave() const {
  remove(PeerId::self());
}

void Discovery::append(const std::string& new_content) const {
  std::ofstream out(kFileName, std::ios::out | std::ios::app);
  out << new_content << "\n";
  out.close();
}

void Discovery::getFileContents(std::string* result) const {
  CHECK_NOTNULL(result);
  std::ifstream in(kFileName, std::ios::in);
  std::string line;
  while (getline(in, line)) {
    if (line != "") {
      *result += line + "\n";
    }
  }
  in.close();
}

void Discovery::lock() {
  while (((lock_file_descriptor_ =
      open(kLockFileName, O_WRONLY | O_EXCL | O_CREAT, 0)) == -1) &&
      errno == EEXIST) {
    usleep(100);
  }
}

void Discovery::replace(const std::string& new_content) const {
  std::ofstream out(kFileName, std::ios::out);
  out << new_content << std::endl;
  out.close();
}

void Discovery::remove(const PeerId& peer) const {
  std::string file_contents;
  getFileContents(&file_contents);
  size_t position = 0;
  while ((position = file_contents.find(peer.ipPort() + "\n"))
      != std::string::npos) {
    file_contents.replace(position, peer.ipPort().length() + 1, "");
  }
  replace(file_contents);
}

void Discovery::unlock() {
  CHECK(close(lock_file_descriptor_) != -1);
  CHECK(unlink(kLockFileName) != -1);
}

const std::string Discovery::kFileName = "/tmp/mapapi-discovery.txt";
const char Discovery::kLockFileName[] = "/tmp/mapapi-discovery.txt.lck";

} /* namespace map_api */
