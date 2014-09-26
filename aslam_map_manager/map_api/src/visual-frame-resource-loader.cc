#include "map-api/visual-frame-resource-loader.h"

namespace map_api {

ResourceLoader::ResourceLoader() {
  // TODO(mfehr): implement
}

const cv::Mat ResourceLoader::loadRawImage() {
  // TODO(mfehr): implement
  return cv::Mat();
}

template <typename ResourceType>
bool ResourceLoader::loadResource(common::VisualFrameBase::Ptr visualFrame,
                                  std::string resourceId) {
  // TODO(mfehr): look up URI for resource id
  // TODO(mfehr): load resource from file system
  // TODO(mfehr): store resource =>
  // visualFrame->storeResource<ResourceType>(std::string resourceId,
  // ResourceType resource)
  // TODO(mfehr): update counter
  // TODO(mfehr): release old resources =>
  // someVisualFrame->releaseResource<ResourceType>(std::string resourceId)
  return true;
};

} /* namespace map_api */
