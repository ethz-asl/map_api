#ifndef MAP_API_VISUAL_FRAME_RESOURCE_LOADER_H_
#define MAP_API_VISUAL_FRAME_RESOURCE_LOADER_H_

#include <multiagent_mapping_common/visual-frame-resource-loader-base.h>

namespace map_api {

class ResourceLoader : public common::ResourceLoaderBase {
 public:
  virtual ~ResourceLoader() {}

  const cv::Mat loadRawImage() {
    cv::Mat* full = new cv::Mat(640, 480);
    return *full;
  };
};

}  // namespace map_api

#endif  // MAP_API_VISUAL_FRAME_RESOURCE_LOADER_H_
