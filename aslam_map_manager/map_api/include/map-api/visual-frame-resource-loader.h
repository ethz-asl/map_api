#ifndef MAP_API_VISUAL_FRAME_RESOURCE_LOADER_H_
#define MAP_API_VISUAL_FRAME_RESOURCE_LOADER_H_

#include <string>

#include <multiagent_mapping_common/visual-frame-resource-loader-base.h>

#include <opencv2/core/core.hpp>

namespace map_api {

class ResourceLoader : public common::ResourceLoaderBase {
 public:
  ResourceLoader();

  virtual ~ResourceLoader() {}

  const cv::Mat loadRawImage();

  template <typename ResourceType>
  bool loadResource(common::VisualFrameBase::Ptr visualFrame,
                    std::string resourceId);

 private:
  // TODO(mfehr): add resource tracking
};

}  // namespace map_api

#endif  // MAP_API_VISUAL_FRAME_RESOURCE_LOADER_H_
