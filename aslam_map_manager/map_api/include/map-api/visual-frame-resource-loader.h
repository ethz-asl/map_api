#ifndef MAP_API_VISUAL_FRAME_RESOURCE_LOADER_H_
#define MAP_API_VISUAL_FRAME_RESOURCE_LOADER_H_

#include <string>
#include <list>
#include <queue>
#include <utility>

#include <opencv2/core/core.hpp>

#include <multiagent_mapping_common/visual-frame-resource-loader-base.h>

#include "map-api/core.h"
#include "map-api/net-table.h"
#include "map-api/net-table-transaction.h"
#include "map-api/transaction.h"
#include "map-api/revision.h"
#include "map-api/ipc.h"
#include "map-api/unique-id.h"
#include "map-api/revision-inl.h"

namespace map_api {

class ResourceLoader : public common::ResourceLoaderBase {
 public:
  typedef common::ResourceLoaderBase::VisualFrameResourceType
      VisualFrameResourceType;

  ResourceLoader();

  virtual ~ResourceLoader() {}

  bool loadResource(std::shared_ptr<common::VisualFrameBase> visualFrame,
                    const std::string visualFrameIdHexString,
                    const std::string resourceId, VisualFrameResourceType type);

  void getResourceIds(const std::string visualFrameIdHexString,
                      VisualFrameResourceType type,
                      std::list<std::string> &idList);

 private:
  void releaseResourcesIfNecessary();
  int increaseResourceCounter(
      VisualFrameResourceType type, std::string resourceId,
      std::shared_ptr<common::VisualFrameBase> visualFramePtr);
  int getNumberOfLoadedResources(VisualFrameResourceType type);
  int releaseNumberOfLoadedResources(VisualFrameResourceType type,
                                     int numberToRelease);

  typedef std::unordered_map<
      int, std::list<std::pair<std::shared_ptr<common::VisualFrameBase>,
                               std::string> > > ResourceMap;
  typedef std::list<std::pair<std::shared_ptr<common::VisualFrameBase>,
                              std::string> > ResourceList;
  typedef std::pair<std::shared_ptr<common::VisualFrameBase>, std::string>
      ResourceRecord;

  ResourceMap loadedResources_;

  static const int MAX_NUMBER_RESOURCES_PER_TYPE = 20;
};

}  // namespace map_api

#endif  // MAP_API_VISUAL_FRAME_RESOURCE_LOADER_H_
