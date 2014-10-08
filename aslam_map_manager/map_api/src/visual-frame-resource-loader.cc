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
  std::shared_ptr<Revision> revision =
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
  if (!revision_map.empty()) {
    int resource_type;
    for (auto revision : revision_map) {
      // TODO(mfehr): replace "1" with proper enum
      revision.second->get<int>(1, &resource_type);
      if (resource_type == type) {
        id_set->insert(revision.first.hexString());
      }
    }
  }
};

int ResourceLoader::registerResource(
    VisualFrameResourceType type, const std::string& resource_id,
    common::VisualFrameBase* visual_frame_ptr) {
  loadedResources_[type]
      .push_back(ResourceRecord(visual_frame_ptr, resource_id));
  return getNumberOfLoadedResources(type);
}

void ResourceLoader::releaseResourcesIfNecessary() {
  for (auto resource_list : loadedResources_) {
    int diff = resource_list.second.size() - kMaxNumberOfResourcesPerType;
    if (diff > 0) {
      CHECK_EQ(
          releaseNumberOfLoadedResources(
              static_cast<VisualFrameResourceType>(resource_list.first), diff),
          diff);
    }
  }
}

int ResourceLoader::getNumberOfLoadedResources(VisualFrameResourceType type)
    const {
  return loadedResources_.at(type).size();
}

int ResourceLoader::releaseNumberOfLoadedResources(VisualFrameResourceType type,
                                                   int numberToRelease) {
  int released = 0;
  ResourceList loaded_resources = loadedResources_.at(type);

  ResourceList::iterator i = loaded_resources.begin();
  while ((i != loaded_resources.end()) && (released < numberToRelease)) {
    CHECK(i->first->releaseResource(i->second));
    i = loaded_resources.erase(i);
    ++released;
  }
  return released;
}

}  // namespace map_api
