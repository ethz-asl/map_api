#include "map-api/visual-frame-resource-loader.h"

namespace map_api {

ResourceLoader::ResourceLoader() {
  // TODO(mfehr): implement
}

template <typename ResourceType>
bool ResourceLoader::loadResource(common::VisualFrameBase::Ptr visualFrame,
                                  std::string resourceId) {
  // TODO(mfehr): check if id is empty, if it is, load first resource
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

template <typename ResourceType>
std::list<std::string> getResourceIds(
    common::VisualFrameBase::Ptr visualFrame) {
  // TODO(mfehr): look up Ids for visual frame and a resource type
  // TODO(mfehr): put in a list and return
  return std::list<std::string>();
};

} /* namespace map_api */
