#ifndef MAP_API_MESSAGE_H_
#define MAP_API_MESSAGE_H_

#include "core.pb.h"

namespace map_api {

class Message : public proto::HubMessage {
 public:
  /**
   * Templated on name because we might want to do different things with the
   * same payload type. Because this is templated on a const char*, the
   * template matches only if the pointer matches: This is great because it
   * forces the use of the messages.h constants. On the other hand, this couples
   * the message type to the string that would be used to identify it, should
   * it be necessary to make a global registry for message types.
   */
  template <const char* message_name, typename PayloadType>
  void impose(const PayloadType& payload);
  /**
   * For payload-free messages, such as ACKs
   */
  template <const char* message_name>
  void impose();

  /**
   * General-purpose message types
   */
  static const char kAck[];
};

#define MAP_API_MESSAGE_IMPOSE_STRING_MESSAGE(message_name) \
    template<> \
    void Message::impose<message_name, \
    std::string>(const std::string& payload) { \
  this->set_name(message_name); \
  this->set_serialized(payload); \
} \
extern void __FILE__ ## __LINE__(void)
// last line swallows the semicolon

} // namespace map_api

#include "map-api/message-inl.h"

#endif /* MAP_API_MESSAGE_H_ */
