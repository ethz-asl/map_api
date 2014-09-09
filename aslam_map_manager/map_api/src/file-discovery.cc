#include "map-api/file-discovery.h"

#include <fstream>
#include <sstream>
#include <string>

#include <sys/file.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "map-api/hub.h"

namespace map_api {

FileDiscovery::~FileDiscovery() {}

void FileDiscovery::announce() { append(Hub::instance().ownAddress()); }

int FileDiscovery::getPeers(std::vector<PeerId>* peers) {
  CHECK_NOTNULL(peers);
  std::string file_contents;
  getFileContents(&file_contents);
  std::istringstream discovery_stream(file_contents);
  std::string address;
  while (discovery_stream >> address) {
    if (address == Hub::instance().ownAddress() || address == "") {
      continue;
    }
    peers->push_back(PeerId(address));
  }
  return peers->size();
}

void FileDiscovery::append(const std::string& new_content) const {
  std::ofstream out(kFileName, std::ios::out | std::ios::app);
  out << new_content << "\n";
  out.close();
}

void FileDiscovery::getFileContents(std::string* result) const {
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

void FileDiscovery::lock() {
  while (((lock_file_descriptor_ =
      open(kLockFileName, O_WRONLY | O_EXCL | O_CREAT, 0)) == -1) &&
      errno == EEXIST) {
    usleep(100);
  }
}

void FileDiscovery::replace(const std::string& new_content) const {
  std::ofstream out(kFileName, std::ios::out);
  out << new_content << std::endl;
  out.close();
}

void FileDiscovery::remove(const PeerId& peer) {
  std::string file_contents;
  getFileContents(&file_contents);
  size_t position = 0;
  while ((position = file_contents.find(peer.ipPort() + "\n"))
      != std::string::npos) {
    file_contents.replace(position, peer.ipPort().length() + 1, "");
  }
  replace(file_contents);
}

void FileDiscovery::unlock() {
  CHECK_NE(close(lock_file_descriptor_), -1);
  CHECK_NE(unlink(kLockFileName), -1);
}

const std::string FileDiscovery::kFileName = "mapapi-discovery.txt";
const char FileDiscovery::kLockFileName[] = "mapapi-discovery.txt.lck";

} /* namespace map_api */
