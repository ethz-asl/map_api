#ifndef DMAP_HUB_INL_H_
#define DMAP_HUB_INL_H_

#include "dmap/message.h"

namespace dmap {

template <typename RequestType, typename ResponseType>
bool Hub::registerHandler(
    void (*handler)(const RequestType& request, ResponseType* response)) {
  // Need to copy handler to avoid reference to temporary.
  return registerHandler(
      UniqueMessageType<RequestType>::message_name, [handler](
          const Message& request_message, Message* response_message){
    CHECK_NOTNULL(response_message);
    RequestType request;
    ResponseType response;
    request_message.extract<UniqueMessageType<RequestType>::message_name>(
        &request);
    handler(request, &response);
    response_message->impose<UniqueMessageType<ResponseType>::message_name>(
        response);
  });
}

template <typename RequestType, typename ResponseType>
void Hub::request(
    const PeerId& peer, const RequestType& request, ResponseType* response) {
  CHECK_NOTNULL(response);
  Message request_message, response_message;
  request_message.impose<UniqueMessageType<RequestType>::message_name>(request);
  this->request(peer, &request_message, &response_message);
  response_message.extract<UniqueMessageType<ResponseType>::message_name>(
      response);
}

}  // namespace dmap

#endif  // DMAP_HUB_INL_H_
