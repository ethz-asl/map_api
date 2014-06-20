#ifndef MAP_API_MESSAGE_H_
#define MAP_API_MESSAGE_H_

#include "core.pb.h"

namespace map_api {

class Message : public proto::HubMessage {
 public:
  /**
   * Templated on type denomination because we might want to do different things
   * with the same payload type. Because this is templated on a const char*, the
   * template matches only if the pointer matches: This is great because it
   * forces the use of unique constants. On the other hand, this couples
   * the message type to the string that is used to identify it, e.g. in the
   * handler map
   */
  template <const char* message_type, typename PayloadType>
  void impose(const PayloadType& payload);
  /**
   * For payload-free messages, such as ACKs
   */
  template <const char* message_type>
  void impose();

  template <const char* message_type>
  bool isType() const;

  /**
   * General-purpose message types
   */
  static const char kAck[];
  static const char kDecline[];
  static const char kInvalid[];
  static const char kRedundant[];
  // not a real response, but indicates failed connection
  static const char kCantReach[];
};

/**
 * Anticipated common payload types: String and ProtoBuf
 */
#define MAP_API_MESSAGE_IMPOSE_STRING_MESSAGE(type_denomination) \
    template<> \
    void Message::impose<type_denomination, \
    std::string>(const std::string& payload) { \
  this->set_type(type_denomination); \
  this->set_serialized(payload); \
} \
extern void __FILE__ ## __LINE__(void) // swallows the semicolon
#define MAP_API_MESSAGE_IMPOSE_PROTO_MESSAGE(type_denomination, proto_type) \
    template<> \
    void Message::impose<type_denomination, \
    proto_type>(const proto_type& payload) { \
  this->set_type(type_denomination); \
  this->set_serialized(payload.SerializeAsString()); \
} \
extern void __FILE__ ## __LINE__(void) // swallows the semicolon

} // namespace map_api

#include "map-api/message-inl.h"

#endif /* MAP_API_MESSAGE_H_ */
