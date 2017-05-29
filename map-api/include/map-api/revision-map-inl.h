#ifndef DMAP_REVISION_MAP_INL_H_
#define DMAP_REVISION_MAP_INL_H_

#include <memory>
#include <unordered_map>
#include <utility>

namespace map_api {

template <typename RevisionType>
template <typename Derived>
typename RevisionMapBase<RevisionType>::iterator
RevisionMapBase<RevisionType>::find(const map_api_common::UniqueId<Derived>& key) {
  map_api_common::Id id_key;
  map_api_common::HashId hash_id;
  key.toHashId(&hash_id);
  id_key.fromHashId(hash_id);
  return find(id_key);
}

template <typename RevisionType>
template <typename Derived>
typename RevisionMapBase<RevisionType>::const_iterator RevisionMapBase<
    RevisionType>::find(const map_api_common::UniqueId<Derived>& key) const {
  map_api_common::Id id_key;
  map_api_common::HashId hash_id;
  key.toHashId(&hash_id);
  id_key.fromHashId(hash_id);  // TODO(tcies) avoid conversion? how?
  return find(id_key);
}

template <typename RevisionType>
std::pair<typename RevisionMapBase<RevisionType>::iterator, bool>
RevisionMapBase<RevisionType>::insert(
    const std::shared_ptr<RevisionType>& revision) {
  CHECK_NOTNULL(revision.get());
  return insert(
      std::make_pair(revision->template getId<map_api_common::Id>(), revision));
}

template <typename RevisionType>
template <typename Derived>
std::pair<typename RevisionMapBase<RevisionType>::iterator, bool>
RevisionMapBase<RevisionType>::insert(
    const map_api_common::UniqueId<Derived>& key,
    const std::shared_ptr<RevisionType>& revision) {
  map_api_common::Id id_key;
  map_api_common::HashId hash_id;
  key.toHashId(&hash_id);
  id_key.fromHashId(hash_id);  // TODO(tcies) avoid conversion? how?
  return insert(std::make_pair(id_key, revision));
}

}  // namespace map_api

#endif  // DMAP_REVISION_MAP_INL_H_
