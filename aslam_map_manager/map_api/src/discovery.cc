#include "map-api/discovery.h"

#include <fstream>
#include <sstream>
#include <string>

#include <sys/file.h> // linux-specific TODO(simon) problem?

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
  std::string file_contents;
  getFileContents(&file_contents);
  size_t position = 0;
  while ((position = file_contents.find(
      MapApiHub::instance().ownAddress() + "\n")) != std::string::npos) {
    file_contents.replace(position,
                          MapApiHub::instance().ownAddress().length() + 1, "");
  }
  replace(file_contents);
}

void Discovery::append(const std::string& new_content) const {
  std::ofstream out(kFileName, std::ios::out | std::ios::app);
  out << new_content << std::endl;
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

void Discovery::lock() const {
  int fd;
  while (((fd = open(kLockFileName, O_WRONLY | O_EXCL | O_CREAT, 0)) == -1)
      && errno == EEXIST) {
    usleep(100);
  }
}

void Discovery::replace(const std::string& new_content) const {
  std::ofstream out(kFileName, std::ios::out);
  out << new_content << std::endl;
  out.close();
}

void Discovery::unlock() const {
  CHECK(unlink(kLockFileName) != -1);
}

const std::string Discovery::kFileName = "/tmp/mapapi-discovery.txt";
const char Discovery::kLockFileName[] = "/tmp/mapapi-discovery.txt.lck";

} /* namespace map_api */
