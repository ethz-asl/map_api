#ifndef MAP_API_RPC_COMMAND_REGISTERER_H_
#define MAP_API_RPC_COMMAND_REGISTERER_H_

#include <multiagent-mapping-common/command-registerer.h>

#include "map-api/message.h"

namespace map_api {
class RpcCommandRegisterer : public common::CommandRegisterer {
 public:
  RpcCommandRegisterer();

  void handleStatusRpc(const Message& request, Message* response);

  /**
   * Handler needs to be registered by application / executable.
   */
  static const char kCommandRequest[];
  static const char kCommandResponse[];
};
}  // namespace map_api
#endif  // MAP_API_RPC_COMMAND_REGISTERER_H_
