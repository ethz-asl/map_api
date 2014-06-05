#ifndef MAP_API_MESSAGE_INL_H_
#define MAP_API_MESSAGE_INL_H_

namespace map_api {

template <const char* message_type>
void Message::impose() {
  this->set_type(message_type);
  this->set_serialized("");
}

} // namespace map_api

#endif /* MAP_API_MESSAGE_INL_H_ */
