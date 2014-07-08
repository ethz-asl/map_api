#ifndef MAP_API_APPLICATION_CONFIGURATION_H_
#define MAP_API_APPLICATION_CONFIGURATION_H_

namespace map_api {

/**
 * For now merely suggestive class for users to centralize table definitions in
 * a class. It is recommended to declare NetTable pointers as public members of
 * this class.
 */
class ApplicationConfiguration {
 public:
  virtual void init() = 0;
};

} /* namespace map_api */

#endif /* MAP_API_APPLICATION_CONFIGURATION_H_ */
