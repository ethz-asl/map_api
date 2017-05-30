// Copyright (C) 2014-2017 Titus Cieslewski, ASL, ETH Zurich, Switzerland
// You can contact the author at <titus at ifi dot uzh dot ch>
// Copyright (C) 2014-2015 Simon Lynen, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014-2015, Marcin Dymczyk, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014, Stéphane Magnenat, ASL, ETH Zurich, Switzerland
//
// This file is part of Map API.
//
// Map API is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Map API is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Map API.  If not, see <http://www.gnu.org/licenses/>.

#ifndef MAP_API_HUB_INL_H_
#define MAP_API_HUB_INL_H_

#include "map-api/message.h"

namespace map_api {

template <typename RequestType>
bool Hub::ackRequest(const PeerId& peer, const RequestType& request) {
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
          const Message& request_message, Message* response_message) {
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

}  // namespace map_api

#endif  // MAP_API_HUB_INL_H_
