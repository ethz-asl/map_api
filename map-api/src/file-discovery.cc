#include "map-api/file-discovery.h"

#include <chrono>
#include <fstream>  // NOLINT
#include <sstream>  // NOLINT
#include <string>
#include <sys/file.h>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <multiagent-mapping-common/conversions.h>
#include <multiagent-mapping-common/delayed-notification.h>

#include "map-api/hub.h"

DEFINE_bool(clear_discovery, false, "Will clear file discovery at startup.");
DEFINE_double(discovery_timeout_seconds, 0.5, "Timeout for file discovery.");

namespace map_api {

FileDiscovery::FileDiscovery()
    : force_unlocked_once_(false) {
  if (FLAGS_clear_discovery) {
    LOG(WARNING) << "Beware, discovery file is manually removed!";
    if (unlink(kFileName) == -1) {
      CHECK_EQ(errno, ENOENT) << errno;
    }
    if (unlink(kLockFileName) == -1) {
      CHECK_EQ(errno, ENOENT) << errno;
    }
  }
}

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
  VLOG(4) << "Appended" << new_content;
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
  mutex_.lock();
  using std::chrono::steady_clock;
  steady_clock::time_point start = steady_clock::now();
  while (true) {
    {
      bool status =
          ((lock_file_descriptor_ =
                open(kLockFileName, O_WRONLY | O_EXCL | O_CREAT, 0)) == -1) &&
          errno == EEXIST;
      if (!status) {
        break;
      }
    }
    usleep(1e4);
    steady_clock::time_point end = steady_clock::now();
    using std::chrono::duration_cast;
    double time_ms =
        duration_cast<std::chrono::milliseconds>(end - start).count();
    if (time_ms > FLAGS_discovery_timeout_seconds * kSecondsToMilliSeconds) {
      // Allow to force unlock the file once, in case there is still a lock file present from
      // a previous unclean shutdown.
      if (!force_unlocked_once_) {
        LOG(ERROR) << "File discovery lock timed out! "
            << "Probably there was an outdated lock file present: " << kLockFileName << ". "
            << "The lock file has been deleted and ownership of the lock will be forced.";
        CHECK_NE(unlink(kLockFileName), -1);
        force_unlocked_once_ = true;
      } else {
        LOG(FATAL) << "File discovery lock timed out! ";
      }
    }
  }
}

void FileDiscovery::replace(const std::string& new_content) const {
  std::ofstream out(kFileName, std::ios::out);
  VLOG(4) << "Replacing " << new_content;
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
  CHECK_NE(close(lock_file_descriptor_), -1) << errno;
  unlink(kLockFileName);
  mutex_.unlock();
}

const char FileDiscovery::kFileName[] = "mapapi-discovery.txt";
const char FileDiscovery::kLockFileName[] = "mapapi-discovery.txt.lck";
std::mutex FileDiscovery::mutex_;

} /* namespace map_api */
