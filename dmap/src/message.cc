#include <dmap/message.h>
#include <string>

#include <glog/logging.h>

namespace dmap {

const char Message::kAck[] = "dmap_msg_ack";
const char Message::kDecline[] = "dmap_msg_decline";
const char Message::kInvalid[] = "dmap_msg_invalid";
const char Message::kRedundant[] = "dmap_msg_redundant";

}  // namespace dmap
