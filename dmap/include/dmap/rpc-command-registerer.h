#ifndef DMAP_RPC_COMMAND_REGISTERER_H_
#define DMAP_RPC_COMMAND_REGISTERER_H_

#include <string>

#include <multiagent-mapping-common/command-registerer.h>

namespace dmap {

class Message;
class PeerId;

class RpcCommandRegisterer : public common::CommandRegisterer {
 public:
  RpcCommandRegisterer();

  void handleStatusRpc(const Message& request, Message* response);

  static int commandRpc(const std::string& command, const PeerId& peer);

  static const char kCommandRequest[];
  static const char kCommandResponse[];
};
}  // namespace dmap
#endif  // DMAP_RPC_COMMAND_REGISTERER_H_
