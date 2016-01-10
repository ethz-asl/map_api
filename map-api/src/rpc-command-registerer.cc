#include "map-api/rpc-command-registerer.h"

#include "map-api/hub.h"
#include "map-api/message.h"
#include "./command-rpc.pb.h"

namespace map_api {
const char RpcCommandRegisterer::kCommandRequest[] =
    "visual_inertial_mapping_command_request";
const char RpcCommandRegisterer::kCommandResponse[] =
    "visual_inertial_mapping_command_response";

MAP_API_STRING_MESSAGE(RpcCommandRegisterer::kCommandRequest);
MAP_API_PROTO_MESSAGE(RpcCommandRegisterer::kCommandResponse,
                      proto::CommandRpcResponse);

RpcCommandRegisterer::RpcCommandRegisterer() : common::CommandRegisterer() {
  Hub::instance().registerHandler(
      kCommandResponse,
      std::bind(&RpcCommandRegisterer::handleStatusRpc, this,
                std::placeholders::_1, std::placeholders::_2));
}

void RpcCommandRegisterer::handleStatusRpc(const Message& request,
                                           Message* response) {
  CHECK_NOTNULL(response);
  proto::CommandRpcResponse command_response;
  request.extract<kCommandResponse>(&command_response);
  printf("Remote command \"%s\" returned status %d\n",
         command_response.command().c_str(), command_response.status());
  response->ack();
}

int RpcCommandRegisterer::commandRpc(const std::string& command,
                                     const PeerId& peer) {
  Message request, response;
  request.impose<kCommandRequest>(command);
  Hub::instance().request(peer, &request, &response);
  CHECK(response.isType<Message::kAck>());
  return common::kSuccess;
}

}  // namespace map_api
