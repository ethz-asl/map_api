#ifndef MAP_API_MESSAGE_INL_H_
#define MAP_API_MESSAGE_INL_H_

namespace map_api {

template <const char* message_name>
void Message::impose() {
  this->set_name(message_name);
  this->set_serialized("");
}

} // namespace map_api

#endif /* MAP_API_MESSAGE_INL_H_ */
