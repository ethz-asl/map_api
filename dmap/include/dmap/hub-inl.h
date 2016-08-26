#ifndef DMAP_HUB_INL_H_
#define DMAP_HUB_INL_H_

#include "dmap/message.h"

namespace dmap {

template <typename RequestType>
bool Hub::ackRequest(const PeerId& peer, const RequestType& request)
{
  Message request_message, response_message;
  request_message.impose<Message::UniqueType<RequestType>::message_name>(
      request);
  this->request(peer, &request_message, &response_message);
  return response_message.isOk();
}

template <typename RequestType, typename ResponseType>
bool Hub::registerHandler(
    void (*handler)(const RequestType& request, ResponseType* response)) {
  // Need to copy handler to avoid reference to temporary.
  return registerHandler(
      Message::UniqueType<RequestType>::message_name, [handler](
          const Message& request_message, Message* response_message){
    CHECK_NOTNULL(response_message);
    RequestType request;
    ResponseType response;
    request_message.extract<Message::UniqueType<RequestType>::message_name>(
        &request);
    handler(request, &response);
    response_message->impose<Message::UniqueType<ResponseType>::message_name>(
        response);
  });
}
template <typename RequestType>
bool Hub::registerHandler(bool (*handler)(const RequestType& request)) {
  // Need to copy handler to avoid reference to temporary.
  return registerHandler(
      Message::UniqueType<RequestType>::message_name, [handler](
          const Message& request_message, Message* response_message){
    CHECK_NOTNULL(response_message);
    RequestType request;
    request_message.extract<Message::UniqueType<RequestType>::message_name>(
        &request);
    if (handler(request)) {
      response_message->ack();
    } else {
      response_message->decline();
    }
  });
}

template <typename RequestType, typename ResponseType>
void Hub::request(
    const PeerId& peer, const RequestType& request, ResponseType* response) {
  CHECK_NOTNULL(response);
  Message request_message, response_message;
  request_message.impose<Message::UniqueType<RequestType>::message_name>(
      request);
  this->request(peer, &request_message, &response_message);
  response_message.extract<Message::UniqueType<ResponseType>::message_name>(
      response);
}

}  // namespace dmap

#endif  // DMAP_HUB_INL_H_
