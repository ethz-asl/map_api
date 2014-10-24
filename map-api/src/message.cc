#include <map-api/message.h>
#include <string>

#include <glog/logging.h>

namespace map_api {

const char Message::kAck[] = "map_api_msg_ack";
const char Message::kDecline[] = "map_api_msg_decline";
const char Message::kInvalid[] = "map_api_msg_invalid";
const char Message::kRedundant[] = "map_api_msg_redundant";

} // namespace map_api
