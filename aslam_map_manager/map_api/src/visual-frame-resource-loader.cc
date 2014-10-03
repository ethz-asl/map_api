#include "map-api/visual-frame-resource-loader.h"

namespace map_api {

ResourceLoader::ResourceLoader() {
  // TODO(mfehr): setup database connection
}

bool ResourceLoader::loadResource(
    std::shared_ptr<common::VisualFrameBase> visualFrame,
    const std::string visualFrameIdHexString, const std::string resourceId,
    VisualFrameResourceType type) {
  bool success = true;

  // TODO(mfehr): look up URI for resource id
  // TODO(mfehr): load resource from file system

  success &= visualFrame->storeResource(resourceId, cv::Mat());
  increaseResourceCounter(type, resourceId, visualFrame);
  releaseResourcesIfNecessary();
  return success;
};

void ResourceLoader::getResourceIds(const std::string visualFrameIdHexString,
                                    VisualFrameResourceType type,
                                    std::list<std::string> &idList) {
  // TODO(mfehr): look up Ids for visual frame and a resource type
  // TODO(mfehr): put in a list and return

  // TODO(mfehr): REMOVE, MAKES UNIT TESTS RUN THROUGH
  idList = std::list<std::string>();
  idList.push_back("00000000000000000000000000000007");
  idList.push_back("00000000000000000000000000000008");
};

int ResourceLoader::increaseResourceCounter(
    VisualFrameResourceType type, std::string resourceId,
    std::shared_ptr<common::VisualFrameBase> visualFramePtr) {
  if (loadedResources_.find(type) == loadedResources_.end()) {
    loadedResources_[type] = ResourceList();
  }
  loadedResources_.at(type)
      .push_back(ResourceRecord(visualFramePtr, resourceId));
  return getNumberOfLoadedResources(type);
}

void ResourceLoader::releaseResourcesIfNecessary() {
  for (auto resourceList : loadedResources_) {
    int diff = resourceList.second.size() - MAX_NUMBER_RESOURCES_PER_TYPE;
    if (diff > 0) {
      CHECK_EQ(
          releaseNumberOfLoadedResources(
              static_cast<VisualFrameResourceType>(resourceList.first), diff),
          diff);
    }
  }
}

int ResourceLoader::getNumberOfLoadedResources(VisualFrameResourceType type) {
  return loadedResources_.at(type).size();
}

int ResourceLoader::releaseNumberOfLoadedResources(VisualFrameResourceType type,
                                                   int numberToRelease) {
  int released = 0;
  bool success = true;
  ResourceList loaded_resources = loadedResources_.at(type);

  ResourceList::iterator i = loaded_resources.begin();
  while ((i != loaded_resources.end()) && (released < numberToRelease)) {
    CHECK(i->first->releaseResource(i->second));
    loaded_resources.erase(i++);
    ++released;
  }
  return released;
}

} /* namespace map_api */
