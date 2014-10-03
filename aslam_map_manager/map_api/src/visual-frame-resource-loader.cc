#include "map-api/visual-frame-resource-loader.h"

namespace map_api {

ResourceLoader::ResourceLoader() {
  // TODO(mfehr): table name and field names are defined in app.h, move to some
  // place we can reach from here
  resourceTable_ = &NetTableManager::instance().getTable(
                        "visual_inertial_mapping_visual_frame_table");
}

bool ResourceLoader::loadResource(
    std::shared_ptr<common::VisualFrameBase> visualFrame,
    const std::string resourceId, VisualFrameResourceType type) {
  bool success = true;

  Transaction transaction;
  Id resource_id;
  resource_id.fromHexString(resourceId);
  std::shared_ptr<Revision> revision =
      transaction.getById<Id>(resource_id, resourceTable_);
  std::string uri;
  // TODO(mfehr): replace "0" with proper enum
  success &= revision->get<std::string>(0, &uri);
  success &=
      visualFrame->storeResource(resourceId, loadResourceFromUri(uri, type));
  increaseResourceCounter(type, resourceId, visualFrame);
  releaseResourcesIfNecessary();
  return success;
};

cv::Mat ResourceLoader::loadResourceFromUri(std::string uri,
                                            VisualFrameResourceType type) {
  cv::Mat image;
  switch (type) {
    case kVisualFrameResourceDisparityImageType:
    case kVisualFrameResourceRawImageType:
      std::cout << "load image from URI = " << uri << std::endl;
      image = cv::imread(uri);
      break;
    default:
      CHECK(false) << "unknown resource type";
  }
  CHECK(image.data && !image.empty());
  return image;
}

void ResourceLoader::getResourceIds(const std::string visualFrameIdHexString,
                                    VisualFrameResourceType type,
                                    std::list<std::string> &idList) {
  idList = std::list<std::string>();
  Transaction transaction;
  Id visualFrameId;
  visualFrameId.fromHexString(visualFrameIdHexString);
  // TODO(mfehr): replace "2" with proper enum
  CRTable::RevisionMap revision_map =
      transaction.find<Id>(2, visualFrameId, resourceTable_);
  if (revision_map.size() > 0) {
    int resource_type;
    for (auto revision : revision_map) {
      // TODO(mfehr): replace "1" with proper enum
      revision.second->get<int>(1, &resource_type);
      if (resource_type == type) {
        idList.push_back(revision.first.hexString());
      }
    }
  }
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
