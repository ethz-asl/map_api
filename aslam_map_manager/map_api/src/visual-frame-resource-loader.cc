#include "map-api/visual-frame-resource-loader.h"

namespace map_api {

ResourceLoader::ResourceLoader(const std::string& resource_table_name) {
  resourceTable_ = &NetTableManager::instance().getTable(resource_table_name);
}

bool ResourceLoader::loadResource(const std::string& resource_id_hex_string,
                                  VisualFrameResourceType type,
                                  common::VisualFrameBase* visual_frame) {
  bool success = true;

  Transaction transaction;
  ResourceId resource_id;
  resource_id.fromHexString(resource_id_hex_string);
  std::shared_ptr<const Revision> revision =
      transaction.getById<ResourceId>(resource_id, resourceTable_);
  std::string uri;
  // TODO(mfehr): replace "0" with proper enum
  success &= revision->get<std::string>(0, &uri);
  success &= visual_frame->storeResource(resource_id_hex_string,
                                         loadResourceFromUri(uri, type));
  registerResource(type, resource_id_hex_string, visual_frame);
  releaseResourcesIfNecessary();
  return success;
};

cv::Mat ResourceLoader::loadResourceFromUri(const std::string& uri,
                                            VisualFrameResourceType type) {
  cv::Mat image;
  switch (type) {
    case kVisualFrameResourceDisparityImageType:
    // Fall through intended
    case kVisualFrameResourceRawImageType:
      image = cv::imread(uri);
      break;
    default:
      CHECK(false) << "unknown resource type: " << type;
  }
  CHECK(image.data && !image.empty());
  return image;
}

void ResourceLoader::getResourceIdsOfType(
    const std::string& visual_frame_id_hex_string, VisualFrameResourceType type,
    std::unordered_set<std::string>* id_set) {
  CHECK_NOTNULL(id_set);
  id_set->clear();
  Transaction transaction;
  Id visual_frame_id;
  visual_frame_id.fromHexString(visual_frame_id_hex_string);
  // TODO(mfehr): replace "2" with proper enum
  CRTable::RevisionMap revision_map =
      transaction.find<Id>(2, visual_frame_id, resourceTable_);
  int resource_type;
  for (auto revision : revision_map) {
    // TODO(mfehr): replace "1" with proper enum
    revision.second->get<int>(1, &resource_type);
    if (resource_type == type) {
      id_set->insert(revision.first.hexString());
    }
  }
};

int ResourceLoader::registerResource(
    VisualFrameResourceType type, const std::string& resource_id,
    common::VisualFrameBase* visual_frame_ptr) {
  loadedResources_[type].emplace_back(visual_frame_ptr, resource_id);
  return getNumberOfLoadedResources(type);
}

void ResourceLoader::releaseResourcesIfNecessary() {
  for (auto resource_list : loadedResources_) {
    int expected_number_of_released_ressources =
        resource_list.second.size() - kMaxNumberOfResourcesPerType;
    if (expected_number_of_released_ressources > 0) {
      CHECK_EQ(releaseNumberOfLoadedResources(
                   static_cast<VisualFrameResourceType>(resource_list.first),
                   expected_number_of_released_ressources),
               expected_number_of_released_ressources);
    }
  }
}

int ResourceLoader::getNumberOfLoadedResources(VisualFrameResourceType type)
    const {
  return loadedResources_.at(type).size();
}

int ResourceLoader::releaseNumberOfLoadedResources(VisualFrameResourceType type,
                                                   int number_to_release) {
  int released = 0;
  ResourceList::iterator i = loadedResources_.at(type).begin();
  ResourceList::iterator end = loadedResources_.at(type).end();
  while ((i != end) && (released < number_to_release)) {
    CHECK(i->visual_frame_ptr->releaseResource(i->resource_id));
    i = loadedResources_.at(type).erase(i);
    ++released;
  }
  return released;
}

}  // namespace map_api
