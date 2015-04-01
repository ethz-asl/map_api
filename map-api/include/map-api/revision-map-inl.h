#ifndef MAP_API_REVISION_MAP_INL_H_
#define MAP_API_REVISION_MAP_INL_H_

#include <memory>
#include <unordered_map>
#include <utility>

namespace map_api {

template <typename RevisionType>
template <typename Derived>
typename RevisionMapBase<RevisionType>::iterator
RevisionMapBase<RevisionType>::find(const common::UniqueId<Derived>& key) {
  common::Id id_key;
  sm::HashId hash_id;
  key.toHashId(&hash_id);
  id_key.fromHashId(hash_id);
  return find(id_key);
}

template <typename RevisionType>
template <typename Derived>
typename RevisionMapBase<RevisionType>::const_iterator RevisionMapBase<
    RevisionType>::find(const common::UniqueId<Derived>& key) const {
  common::Id id_key;
  sm::HashId hash_id;
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
      std::make_pair(revision->template getId<common::Id>(), revision));
}

template <typename RevisionType>
template <typename Derived>
std::pair<typename RevisionMapBase<RevisionType>::iterator, bool>
RevisionMapBase<RevisionType>::insert(
    const common::UniqueId<Derived>& key,
    const std::shared_ptr<RevisionType>& revision) {
  common::Id id_key;
  sm::HashId hash_id;
  key.toHashId(&hash_id);
  id_key.fromHashId(hash_id);  // TODO(tcies) avoid conversion? how?
  return insert(std::make_pair(id_key, revision));
}

}  // namespace map_api

#endif  // MAP_API_REVISION_MAP_INL_H_
