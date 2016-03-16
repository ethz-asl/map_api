#include "dmap/core.h"

#include <glog/logging.h>

#include "dmap/hub.h"
#include "dmap/ipc.h"
#include "dmap/net-table-manager.h"

namespace dmap {

Core Core::instance_;

Core* Core::instance() {
  std::unique_lock<std::mutex> lock(instance_.initialized_mutex_);
  if (instance_.initialized_) {
    return &instance_;
  } else {
    return nullptr;
  }
}

Core* Core::instanceNoWait() {
  if (!instance_.initialized_mutex_.try_lock()) {
    return nullptr;
  } else {
    if (instance_.initialized_) {
      instance_.initialized_mutex_.unlock();
      return &instance_;
    } else {
      instance_.initialized_mutex_.unlock();
      return nullptr;
    }
  }
}

void Core::initializeInstance() {
  std::unique_lock<std::mutex> lock(instance_.initialized_mutex_);
  CHECK(!instance_.initialized_);
  instance_.init();
  lock.unlock();
  CHECK_NOTNULL(instance());
}

Core::Core()
    : hub_(Hub::instance()),
      table_manager_(NetTableManager::instance()),
      initialized_(false) {}

// can't initialize metatable in init, as its initialization calls
// MapApiCore::getInstance, which again calls this
void Core::init() {
  IPC::registerHandlers();
  NetTableManager::registerHandlers();
  bool is_first_peer;
  if (!hub_.init(&is_first_peer)) {
    LOG(FATAL) << "dmap core init failed";
  }
  // ready metatable
  table_manager_.init(is_first_peer);
  initialized_ = true;
  VLOG(1) << "dmap instance running at address " << PeerId::self();
}

bool Core::isInitialized() const { return initialized_; }

void Core::kill() {
  VLOG(1) << "Killing dmap instance at address " << PeerId::self();
  table_manager_.kill();
  hub_.kill();
  initialized_ = false;  // TODO(tcies) re-order?
}

void Core::killOnceShared() {
  VLOG(1) << "Killing (once shared) dmap instance at " << PeerId::self();
  table_manager_.killOnceShared();
  hub_.kill();
  initialized_ = false;
}

void Core::killOnceSharedUnlessAlone() {
  std::set<PeerId> peers;
  hub_.getPeers(&peers);
  if (peers.empty()) {
    kill();
  } else {
    killOnceShared();
  }
}

Core::~Core() {
  CHECK(initialized_mutex_.try_lock());
  if (initialized_) {
    kill();  // TODO(tcies) could fail - require of user to invoke instead?
  }
}

}  // namespace dmap
