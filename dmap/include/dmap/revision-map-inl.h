#ifndef DMAP_REVISION_MAP_INL_H_
#define DMAP_REVISION_MAP_INL_H_

#include <memory>
#include <unordered_map>
#include <utility>

namespace dmap {

template <typename RevisionType>
template <typename Derived>
typename RevisionMapBase<RevisionType>::iterator
RevisionMapBase<RevisionType>::find(const dmap_common::UniqueId<Derived>& key) {
  dmap_common::Id id_key;
  dmap_common::HashId hash_id;
  key.toHashId(&hash_id);
  id_key.fromHashId(hash_id);
  return find(id_key);
}

template <typename RevisionType>
template <typename Derived>
typename RevisionMapBase<RevisionType>::const_iterator RevisionMapBase<
    RevisionType>::find(const dmap_common::UniqueId<Derived>& key) const {
  dmap_common::Id id_key;
  dmap_common::HashId hash_id;
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
      std::make_pair(revision->template getId<dmap_common::Id>(), revision));
}

template <typename RevisionType>
template <typename Derived>
std::pair<typename RevisionMapBase<RevisionType>::iterator, bool>
RevisionMapBase<RevisionType>::insert(
    const dmap_common::UniqueId<Derived>& key,
    const std::shared_ptr<RevisionType>& revision) {
  dmap_common::Id id_key;
  dmap_common::HashId hash_id;
  key.toHashId(&hash_id);
  id_key.fromHashId(hash_id);  // TODO(tcies) avoid conversion? how?
  return insert(std::make_pair(id_key, revision));
}

}  // namespace dmap

#endif  // DMAP_REVISION_MAP_INL_H_
