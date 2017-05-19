#ifndef DMAP_REVISION_MAP_H_
#define DMAP_REVISION_MAP_H_

#include <memory>
#include <unordered_map>
#include <utility>

#include <dmap-common/unique-id.h>

#include "dmap/revision.h"

namespace dmap {

template <typename ConstNonConstRevision>
class RevisionMapBase
    : public std::unordered_map<dmap_common::Id,
                                std::shared_ptr<ConstNonConstRevision>> {
 public:
  typedef std::unordered_map<
      dmap_common::Id, std::shared_ptr<ConstNonConstRevision>>
      Base;
  typedef typename Base::iterator iterator;
  typedef typename Base::const_iterator const_iterator;

  using Base::find;
  template <typename IdType>
  iterator find(const dmap_common::UniqueId<IdType>& key);
  template <typename IdType>
  const_iterator find(const dmap_common::UniqueId<IdType>& key) const;

  using Base::insert;
  std::pair<iterator, bool> insert(
      const std::shared_ptr<ConstNonConstRevision>& revision);
  template <typename IdType>
  std::pair<typename Base::iterator, bool> insert(
      const dmap_common::UniqueId<IdType>& key,
      const std::shared_ptr<ConstNonConstRevision>& revision);
};

// Using derived classes here allows forward declaration.
class ConstRevisionMap : public RevisionMapBase<const Revision> {};
class MutableRevisionMap : public RevisionMapBase<Revision> {};

}  // namespace dmap

#include "dmap/revision-map-inl.h"

#endif  // DMAP_REVISION_MAP_H_
