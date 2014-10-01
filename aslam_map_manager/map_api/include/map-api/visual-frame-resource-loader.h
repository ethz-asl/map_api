#ifndef MAP_API_VISUAL_FRAME_RESOURCE_LOADER_H_
#define MAP_API_VISUAL_FRAME_RESOURCE_LOADER_H_

#include <string>
#include <list>

#include <multiagent_mapping_common/visual-frame-resource-loader-base.h>

namespace map_api {

class ResourceLoader : public common::ResourceLoaderBase {
 public:
  ResourceLoader();

  virtual ~ResourceLoader() {}

  template <typename ResourceType>
  bool loadResource(common::VisualFrameBase::Ptr visualFrame,
                    std::string resourceId = std::string());

  template <typename ResourceType>
  std::list<std::string> getResourceIds(
      common::VisualFrameBase::Ptr visualFrame);

 private:
  // TODO(mfehr): add resource tracking
};

}  // namespace map_api

#endif  // MAP_API_VISUAL_FRAME_RESOURCE_LOADER_H_
