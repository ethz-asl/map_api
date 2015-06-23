#include "map-api/spatial-index.h"

#include <multiagent-mapping-common/conversions.h>
#include <multiagent-mapping-common/unique-id.h>

#include "map-api/hub.h"
#include "map-api/message.h"
#include "map-api/net-table-manager.h"
#include "./chord-index.pb.h"
#include "./net-table.pb.h"

namespace map_api {

SpatialIndex::BoundingBox::BoundingBox() : std::vector<Range>() {}

SpatialIndex::BoundingBox::BoundingBox(int size) : std::vector<Range>(size) {}

SpatialIndex::BoundingBox::BoundingBox(
    const std::initializer_list<Range>& init_list)
    : std::vector<Range>(init_list) {}

SpatialIndex::BoundingBox::BoundingBox(const Eigen::Vector3d& min,
                                       const Eigen::Vector3d& max)
    : BoundingBox({{min[0], max[0]}, {min[1], max[1]}, {min[2], max[2]}}) {}

std::string SpatialIndex::BoundingBox::debugString() const {
  std::ostringstream ss;
  bool first = true;
  for (Range range : *this) {
    ss << (first ? "" : ",") << range.min << "," << range.max;
    first = false;
  }
  return ss.str();
}

void SpatialIndex::BoundingBox::serialize(
    google::protobuf::RepeatedField<double>* field) const {
  field->Clear();
  for (const Range& range : *this) {
    field->Add(range.min);
    field->Add(range.max);
  }
}

void SpatialIndex::BoundingBox::deserialize(
    const google::protobuf::RepeatedField<double>& field) {
  CHECK_EQ(field.size() % 2u, 0u);
  clear();
  for (int i = 0; i < field.size(); i += 2) {
    push_back(Range(field.Get(i), field.Get(i + 1)));
  }
}

SpatialIndex::SpatialIndex(const std::string& table_name,
                           const BoundingBox& bounds,
                           const std::vector<size_t>& subdivision)
    : table_name_(table_name), bounds_(bounds), subdivision_(subdivision) {
  CHECK_EQ(bounds.size(), subdivision.size());
  for (size_t count : subdivision) {
    CHECK_GT(count, 0u);
  }
  for (const Range& bound : bounds) {
    CHECK_LT(bound.min, bound.max);
  }
}

SpatialIndex::~SpatialIndex() {}

void SpatialIndex::create() {
  ChordIndex::create();
  SpatialIndexCellData empty_data;
  for (const Cell& cell : *this) {
    CHECK(addData(cell.chordKey(), empty_data.SerializeAsString()));
  }
}

void SpatialIndex::announceChunk(const common::Id& chunk_id,
                                 const BoundingBox& bounding_box) {
  std::vector<Cell> affected_cells;
  getCellsInBoundingBox(bounding_box, &affected_cells);

  for (Cell& cell : affected_cells) {
    if (VLOG_IS_ON(3)) {
      Eigen::AlignedBox3d box;
      cell.getDimensions(&box);
      VLOG(3) << "Pushing chunk " << chunk_id << " to " << box.min().transpose()
              << " :: " << box.max().transpose();
    }
    cell.accessor().get().addChunkIdIfNotPresent(chunk_id);
  }
}

void SpatialIndex::seekChunks(const BoundingBox& bounding_box,
                              common::IdSet* chunk_ids) {
  CHECK_NOTNULL(chunk_ids);
  std::vector<Cell> affected_cells;
  getCellsInBoundingBox(bounding_box, &affected_cells);

  for (Cell& cell : affected_cells) {
    cell.constPatientAccessor(1000).get().addChunkIds(chunk_ids);
  }
}

void SpatialIndex::listenToSpace(const BoundingBox& bounding_box) {
  std::vector<Cell> affected_cells;
  getCellsInBoundingBox(bounding_box, &affected_cells);

  for (Cell& cell : affected_cells) {
    cell.announceAsListener();
  }
}

SpatialIndex::Cell::Cell(size_t position_1d, SpatialIndex* index)
    : position_1d_(position_1d), index_(CHECK_NOTNULL(index)) {}

// Meta-information.
void SpatialIndex::Cell::getDimensions(Eigen::AlignedBox3d* result) {
  CHECK_NOTNULL(result);
  Eigen::Vector3d min_corner;
  CHECK_EQ(index_->subdivision_.size(), 3u)
      << "Higher dimensions not supported yet!";
  // "z" is least significant.
  size_t remainder = position_1d_;
  Eigen::Vector3i position_3d_;
  for (int dim = index_->subdivision_.size() - 1; dim >= 0; --dim) {
    position_3d_[dim] = remainder % index_->subdivision_[dim];
    remainder /= index_->subdivision_[dim];
  }
  CHECK_EQ(remainder, 0u);
  // TODO(tcies) consolidate with nidegens spatial index functions into common.
  const Eigen::Vector3d full_extent(
      index_->bounds_[0].max - index_->bounds_[0].min,
      index_->bounds_[1].max - index_->bounds_[1].min,
      index_->bounds_[2].max - index_->bounds_[2].min);
  const Eigen::Vector3i subdivision(index_->subdivision_[0],
                                    index_->subdivision_[1],
                                    index_->subdivision_[2]);
  Eigen::Vector3d unit_cell_extent =
      full_extent.cwiseQuotient(subdivision.cast<double>());
  for (int dim = 0; dim < 3; ++dim) {
    min_corner[dim] =
        index_->bounds_[dim].min + position_3d_[dim] * unit_cell_extent[dim];
  }

  result->min() = min_corner;
  result->max() = min_corner + unit_cell_extent;
}

std::string SpatialIndex::Cell::chordKey() const {
  return positionToKey(position_1d_);
}

SpatialIndex::Cell& SpatialIndex::Cell::operator++() {
  ++position_1d_;
  return *this;
}

bool SpatialIndex::Cell::operator!=(const Cell& other) {
  return position_1d_ != other.position_1d_;
}

void SpatialIndex::Cell::announceAsListener() {
  accessor().get().addListenerIfNotPresent(PeerId::self());
}

void SpatialIndex::Cell::getListeners(PeerIdSet* result) {
  CHECK_NOTNULL(result)->clear();
  constAccessor().get().getListeners(result);
}

SpatialIndex::Cell::Accessor::Accessor(Cell* cell) : Accessor(cell, 0u) {}

SpatialIndex::Cell::Accessor::Accessor(Cell* cell, size_t timeout_ms)
    : cell_(*CHECK_NOTNULL(cell)), data_(), dirty_(false) {
  std::string data_string;
  if (timeout_ms != 0u) {
    if (!cell_.index_->retrieveData(positionToKey(cell_.position_1d_),
                                    &data_string)) {
      usleep(timeout_ms * kMillisecondsToMicroseconds);
      CHECK(cell_.index_->retrieveData(positionToKey(cell_.position_1d_),
                                       &data_string));
    }
  } else {
    CHECK(cell_.index_->retrieveData(positionToKey(cell_.position_1d_),
                                     &data_string));
  }
  CHECK(data_.ParseFromString(data_string));
}

SpatialIndex::Cell::Accessor::~Accessor() {
  if (dirty_) {
    CHECK(cell_.index_->addData(positionToKey(cell_.position_1d_),
                                data_.SerializeAsString()));
  }
}

size_t SpatialIndex::size() const {
  size_t result = 1;
  for (size_t dimension_size : subdivision_) {
    CHECK_NE(dimension_size, 0u);
    result *= dimension_size;
  }
  return result;
}

SpatialIndex::Cell SpatialIndex::begin() { return Cell(0u, this); }

SpatialIndex::Cell SpatialIndex::end() {
  CHECK_EQ(subdivision_.size(), 3u);
  return Cell(size(), this);
}

const char SpatialIndex::kRoutedChordRequest[] =
    "map_api_spatial_index_request";
// Because the requests are routed, we don't need to be careful with the choice
// of name!
const char SpatialIndex::kPeerResponse[] = "peer_response";
const char SpatialIndex::kGetClosestPrecedingFingerRequest[] =
    "get_closest_preceding_finger_request";
const char SpatialIndex::kGetSuccessorRequest[] = "get_successor_request";
const char SpatialIndex::kGetPredecessorRequest[] = "get_predecessor_request";
const char SpatialIndex::kLockRequest[] = "lock_request";
const char SpatialIndex::kUnlockRequest[] = "unlock_request";
const char SpatialIndex::kNotifyRequest[] = "notify_request";
const char SpatialIndex::kReplaceRequest[] = "replace_request";
const char SpatialIndex::kAddDataRequest[] = "add_data_request";
const char SpatialIndex::kRetrieveDataRequest[] = "retrieve_data_request";
const char SpatialIndex::kRetrieveDataResponse[] = "retrieve_data_response";
const char SpatialIndex::kFetchResponsibilitiesRequest[] =
    "fetch_responsibilities_request";
const char SpatialIndex::kFetchResponsibilitiesResponse[] =
    "fetch_responsibilities_response";
const char SpatialIndex::kPushResponsibilitiesRequest[] =
    "push_responsibilities_response";
const char SpatialIndex::kInitReplicatorRequest[] =
    "init_chord_replicator";
const char SpatialIndex::kAppendReplicationDataRequest[] =
    "append_chord_replication_data";
const char SpatialIndex::kFetchReplicationDataRequest[] =
    "fetch_chord_replication_data";
const char SpatialIndex::kFetchReplicationDataResponse[] =
    "fetch_chord_replication_data_response";

MAP_API_PROTO_MESSAGE(SpatialIndex::kRoutedChordRequest,
                      proto::RoutedChordRequest);

MAP_API_STRING_MESSAGE(SpatialIndex::kPeerResponse);
MAP_API_STRING_MESSAGE(SpatialIndex::kGetClosestPrecedingFingerRequest);
MAP_API_STRING_MESSAGE(SpatialIndex::kNotifyRequest);
MAP_API_PROTO_MESSAGE(SpatialIndex::kReplaceRequest, proto::ReplaceRequest);
MAP_API_PROTO_MESSAGE(SpatialIndex::kAddDataRequest, proto::AddDataRequest);
MAP_API_STRING_MESSAGE(SpatialIndex::kRetrieveDataRequest);
MAP_API_STRING_MESSAGE(SpatialIndex::kRetrieveDataResponse);
MAP_API_PROTO_MESSAGE(SpatialIndex::kFetchResponsibilitiesResponse,
                      proto::FetchResponsibilitiesResponse);
MAP_API_PROTO_MESSAGE(SpatialIndex::kPushResponsibilitiesRequest,
                      proto::FetchResponsibilitiesResponse);
MAP_API_PROTO_MESSAGE(SpatialIndex::kInitReplicatorRequest,
                      proto::FetchResponsibilitiesResponse);
MAP_API_PROTO_MESSAGE(SpatialIndex::kAppendReplicationDataRequest,
                      proto::FetchResponsibilitiesResponse);
MAP_API_PROTO_MESSAGE(SpatialIndex::kFetchReplicationDataRequest,
                      proto::FetchReplicationDataRequest);
MAP_API_PROTO_MESSAGE(SpatialIndex::kFetchReplicationDataResponse,
                      proto::FetchResponsibilitiesResponse);

void SpatialIndex::handleRoutedRequest(const Message& routed_request_message,
                                       Message* response) {
  CHECK_NOTNULL(response);
  proto::RoutedChordRequest routed_request;
  routed_request_message.extract<kRoutedChordRequest>(&routed_request);
  CHECK(routed_request.has_serialized_message());
  Message request;
  CHECK(request.ParseFromString(routed_request.serialized_message()));
  // TODO(tcies) a posteriori, especially given the new routing system,
  // map_api::Message handling in ChordIndex itself could have been a thing
  // the following code is mostly copied from test/test_chord_index.cpp :(

  if (!request.has_sender()) {
    CHECK(routed_request_message.has_sender());
    request.setSender(routed_request_message.sender());
  }

  if (request.isType<kGetClosestPrecedingFingerRequest>()) {
    Key key;
    std::istringstream key_ss(request.serialized());
    key_ss >> key;
    std::ostringstream peer_ss;
    PeerId closest_preceding;
    if (!handleGetClosestPrecedingFinger(key, &closest_preceding)) {
      response->decline();
      return;
    }
    peer_ss << closest_preceding.ipPort();
    response->impose<kPeerResponse>(peer_ss.str());
    return;
  }

  if (request.isType<kGetSuccessorRequest>()) {
    PeerId successor;
    if (!handleGetSuccessor(&successor)) {
      response->decline();
      return;
    }
    response->impose<kPeerResponse>(successor.ipPort());
    return;
  }

  if (request.isType<kGetPredecessorRequest>()) {
    PeerId predecessor;
    if (!handleGetPredecessor(&predecessor)) {
      response->decline();
      return;
    }
    response->impose<kPeerResponse>(predecessor.ipPort());
    return;
  }

  if (request.isType<kLockRequest>()) {
    PeerId requester(request.sender());
    if (handleLock(requester)) {
      response->ack();
    } else {
      response->decline();
    }
    return;
  }

  if (request.isType<kUnlockRequest>()) {
    PeerId requester(request.sender());
    if (handleUnlock(requester)) {
      response->ack();
    } else {
      response->decline();
    }
    return;
  }

  if (request.isType<kNotifyRequest>()) {
    if (handleNotify(PeerId(request.serialized()))) {
      response->ack();
    } else {
      response->decline();
    }
    return;
  }

  if (request.isType<kReplaceRequest>()) {
    proto::ReplaceRequest replace_request;
    request.extract<kReplaceRequest>(&replace_request);
    if (handleReplace(PeerId(replace_request.old_peer()),
                      PeerId(replace_request.new_peer()))) {
      response->ack();
    } else {
      response->decline();
    }
    return;
  }

  if (request.isType<kAddDataRequest>()) {
    proto::AddDataRequest add_data_request;
    request.extract<kAddDataRequest>(&add_data_request);
    CHECK(add_data_request.has_key());
    CHECK(add_data_request.has_value());
    if (handleAddData(add_data_request.key(), add_data_request.value())) {
      response->ack();
    } else {
      response->decline();
    }
    return;
  }

  if (request.isType<kRetrieveDataRequest>()) {
    std::string key, value;
    request.extract<kRetrieveDataRequest>(&key);
    if (handleRetrieveData(key, &value)) {
      response->impose<kRetrieveDataResponse>(value);
    } else {
      response->decline();
    }
    return;
  }

  if (request.isType<kFetchResponsibilitiesRequest>()) {
    DataMap data;
    PeerId requester = PeerId(request.sender());
    CHECK(request.isType<kFetchResponsibilitiesRequest>());
    if (handleFetchResponsibilities(requester, &data)) {
      proto::FetchResponsibilitiesResponse fetch_response;
      for (const DataMap::value_type& item : data) {
        proto::AddDataRequest add_request;
        add_request.set_key(item.first);
        add_request.set_value(item.second);
        proto::AddDataRequest* slot = fetch_response.add_data();
        CHECK_NOTNULL(slot);
        *slot = add_request;
      }
      response->impose<kFetchResponsibilitiesResponse>(fetch_response);
    } else {
      response->decline();
    }
    return;
  }

  if (request.isType<kPushResponsibilitiesRequest>()) {
    DataMap data;
    proto::FetchResponsibilitiesResponse push_request;
    request.extract<kPushResponsibilitiesRequest>(&push_request);
    for (int i = 0; i < push_request.data_size(); ++i) {
      data[push_request.data(i).key()] = push_request.data(i).value();
    }
    if (handlePushResponsibilities(data)) {
      response->ack();
    } else {
      response->decline();
    }
    return;
  }

  if (request.isType<kInitReplicatorRequest>()) {
    DataMap data;
    proto::FetchResponsibilitiesResponse init_request;
    request.extract<kInitReplicatorRequest>(&init_request);
    for (int i = 0; i < init_request.data_size(); ++i) {
      data[init_request.data(i).key()] = init_request.data(i).value();
    }
    if (handleInitReplicator(
          init_request.replicator_index(), data, request.sender())) {
      response->ack();
    } else {
      response->decline();
    }
    return;
  }

  if (request.isType<kAppendReplicationDataRequest>()) {
    DataMap data;
    proto::FetchResponsibilitiesResponse replication_request;
    request.extract<kAppendReplicationDataRequest>(&replication_request);
    for (int i = 0; i < replication_request.data_size(); ++i) {
      data[replication_request.data(i).key()] = replication_request.data(i).value();
    }
    if (handleAppendReplicationData(
          replication_request.replicator_index(), data, request.sender())) {
      response->ack();
    } else {
      response->decline();
    }
    return;
  }

  if (request.isType<kFetchReplicationDataRequest>()) {
    DataMap data;
    PeerId replicating_peer_;
    CHECK(request.isType<kFetchReplicationDataRequest>());
    proto::FetchReplicationDataRequest data_request;
    request.extract<kFetchReplicationDataRequest>(&data_request);
    if (handleFetchReplicationData(
          data_request.replicator_index(), &data, &replicating_peer_)) {
      proto::FetchResponsibilitiesResponse fetch_response;
      fetch_response.set_replicator_peer_id(replicating_peer_.ipPort());
      for (const DataMap::value_type& item : data) {
        proto::AddDataRequest add_request;
        add_request.set_key(item.first);
        add_request.set_value(item.second);
        proto::AddDataRequest* slot = fetch_response.add_data();
        CHECK_NOTNULL(slot);
        // TODO(aqurai): Why?
        *slot = add_request;
      }
      response->impose<kFetchReplicationDataResponse>(fetch_response);
    } else {
      response->decline();
    }
    return;
  }

  LOG(FATAL) << "Net table index can't handle request of type "
             << request.type();
}

inline void SpatialIndex::getCellsInBoundingBox(const BoundingBox& bounding_box,
                                                std::vector<Cell>* cells) {
  CHECK_NOTNULL(cells)->clear();
  CHECK_EQ(bounds_.size(), bounding_box.size());
  std::vector<size_t> indices;
  // The following loop iterates over the dimensions to obtain the
  // multi-dimensional set of indices corresponding to the bounding box.
  // Each iteration can be considered an extrusion of the lower-dimensional
  // result.
  // For instance, to obtain:
  // ...
  // ...
  // .##
  // .##
  // which corresponds to {7, 8, 10, 11}, we would first obtain the indices in
  // one dimension, for instance
  // .##
  // or {1, 2}
  // In the next iteration, we detect the bounding box in the other dimension,
  // ..## or {2, 3}. To obtain the result for both dimensions jointly,
  // we add to each lower dimensional index a higher dimensional index times
  // the total size of the lower dimension, in this case:
  // {1, 2} + 3 * {2, 3} = {1 + 3 * 2, 2 + 3 * 2, 1 + 3 * 3, 2 + 3 * 3} =
  // {7, 8, 10, 11}
  // This can then be continued for higher dimensions.
  // In particular, x is most significant while z is least significant.
  size_t lower_dimensions_size = 1;
  indices.push_back(0);
  for (size_t dimension = 0; dimension < bounds_.size(); ++dimension) {
    CHECK_GE(bounding_box[dimension].min, bounds_[dimension].min);
    CHECK_LT(bounding_box[dimension].min, bounding_box[dimension].max);
    CHECK_LE(bounding_box[dimension].max, bounds_[dimension].max);
    // indices in this dimension
    std::vector<size_t> this_dimension_indices;
    for (size_t i = coefficientOf(dimension, bounding_box[dimension].min);
         i <= coefficientOf(dimension, bounding_box[dimension].max); ++i) {
      this_dimension_indices.push_back(i);
    }
    // extrusion
    std::vector<size_t> extrusion_step;
    for (size_t this_dimension_index : this_dimension_indices) {
      for (size_t previous_index : indices) {
        extrusion_step.push_back(previous_index +
                                 this_dimension_index * lower_dimensions_size);
      }
    }
    indices.swap(extrusion_step);
    lower_dimensions_size *= subdivision_[dimension];
  }
  for (size_t index : indices) {
    cells->push_back(Cell(index, this));
  }
}

inline size_t SpatialIndex::coefficientOf(size_t dimension,
                                          double value) const {
  if (value == bounds_[dimension].max) {
    return subdivision_[dimension] - 1;
  }
  value -= bounds_[dimension].min;
  value *= subdivision_[dimension];
  value /= (bounds_[dimension].max - bounds_[dimension].min);
  return static_cast<size_t>(value);
}

inline std::string SpatialIndex::positionToKey(size_t cell_index) {
  return std::to_string(cell_index);
}

inline size_t SpatialIndex::keyToPosition(const std::string& key) {
  return std::stoul(key);
}

// ========
// REQUESTS
// ========
ChordIndex::RpcStatus SpatialIndex::rpc(const PeerId& to,
                                        const Message& request,
                                        Message* response) {
  CHECK_NOTNULL(response);
  Message to_be_sent;
  proto::RoutedChordRequest routed_request;
  routed_request.set_table_name(table_name_);
  routed_request.set_serialized_message(request.SerializeAsString());
  to_be_sent.impose<kRoutedChordRequest>(routed_request);
  if (!peers_.try_request(to, &to_be_sent, response)) {
    return RpcStatus::RPC_FAILED;
  }
  if (response->isType<Message::kDecline>()) {
    return RpcStatus::DECLINED;
  }
  return RpcStatus::SUCCESS;
}

bool SpatialIndex::getClosestPrecedingFingerRpc(const PeerId& to,
                                                const Key& key,
                                                PeerId* result) {
  CHECK_NOTNULL(result);
  Message request, response;
  std::ostringstream key_ss;
  key_ss << key;
  request.impose<kGetClosestPrecedingFingerRequest>(key_ss.str());
  if (rpc(to, request, &response) != RpcStatus::SUCCESS) {
    return false;
  }
  CHECK(response.isType<kPeerResponse>());
  *result = PeerId(response.serialized());
  return true;
}

bool SpatialIndex::getSuccessorRpc(const PeerId& to, PeerId* result) {
  CHECK_NOTNULL(result);
  Message request, response;
  request.impose<kGetSuccessorRequest>();
  if (rpc(to, request, &response) != RpcStatus::SUCCESS) {
    return false;
  }
  CHECK(response.isType<kPeerResponse>());
  *result = PeerId(response.serialized());
  return true;
}

bool SpatialIndex::getPredecessorRpc(const PeerId& to, PeerId* result) {
  CHECK_NOTNULL(result);
  Message request, response;
  request.impose<kGetPredecessorRequest>();
  if (rpc(to, request, &response) != RpcStatus::SUCCESS) {
    return false;
  }
  CHECK(response.isType<kPeerResponse>());
  *result = PeerId(response.serialized());
  return true;
}

ChordIndex::RpcStatus SpatialIndex::lockRpc(const PeerId& to) {
  Message request, response;
  request.impose<kLockRequest>();
  return rpc(to, request, &response);
}

ChordIndex::RpcStatus SpatialIndex::unlockRpc(const PeerId& to) {
  Message request, response;
  request.impose<kUnlockRequest>();
  return rpc(to, request, &response);
}

bool SpatialIndex::notifyRpc(const PeerId& to, const PeerId& self) {
  Message request, response;
  request.impose<kNotifyRequest>(self.ipPort());
  if (rpc(to, request, &response) != RpcStatus::SUCCESS) {
    return false;
  }
  CHECK(response.isType<Message::kAck>());
  return true;
}

bool SpatialIndex::replaceRpc(const PeerId& to, const PeerId& old_peer,
                              const PeerId& new_peer) {
  Message request, response;
  proto::ReplaceRequest replace_request;
  replace_request.set_old_peer(old_peer.ipPort());
  replace_request.set_new_peer(new_peer.ipPort());
  request.impose<kReplaceRequest>(replace_request);
  if (rpc(to, request, &response) != RpcStatus::SUCCESS) {
    return false;
  }
  CHECK(response.isType<Message::kAck>());
  return true;
}

bool SpatialIndex::addDataRpc(const PeerId& to, const std::string& key,
                              const std::string& value) {
  Message request, response;
  proto::AddDataRequest add_data_request;
  add_data_request.set_key(key);
  add_data_request.set_value(value);
  request.impose<kAddDataRequest>(add_data_request);
  if (rpc(to, request, &response) != RpcStatus::SUCCESS) {
    return false;
  }
  CHECK(response.isType<Message::kAck>());
  return true;
}

bool SpatialIndex::retrieveDataRpc(const PeerId& to, const std::string& key,
                                   std::string* value) {
  CHECK_NOTNULL(value);
  Message request, response;
  request.impose<kRetrieveDataRequest>(key);
  if (rpc(to, request, &response) != RpcStatus::SUCCESS) {
    return false;
  }
  CHECK(response.isType<kRetrieveDataResponse>());
  response.extract<kRetrieveDataResponse>(value);
  return true;
}

bool SpatialIndex::fetchResponsibilitiesRpc(const PeerId& to,
                                            DataMap* responsibilities) {
  CHECK_NOTNULL(responsibilities);
  Message request, response;
  request.impose<kFetchResponsibilitiesRequest>();
  if (rpc(to, request, &response) != RpcStatus::SUCCESS) {
    return false;
  }
  CHECK(response.isType<kFetchResponsibilitiesResponse>());
  proto::FetchResponsibilitiesResponse fetch_response;
  response.extract<kFetchResponsibilitiesResponse>(&fetch_response);
  for (int i = 0; i < fetch_response.data_size(); ++i) {
    responsibilities->emplace(fetch_response.data(i).key(),
                              fetch_response.data(i).value());
  }
  return true;
}

bool SpatialIndex::pushResponsibilitiesRpc(const PeerId& to,
                                           const DataMap& responsibilities) {
  Message request, response;
  proto::FetchResponsibilitiesResponse push_request;
  for (const DataMap::value_type& item : responsibilities) {
    proto::AddDataRequest* slot = push_request.add_data();
    slot->set_key(item.first);
    slot->set_value(item.second);
  }
  request.impose<kPushResponsibilitiesRequest>(push_request);
  if (rpc(to, request, &response) != RpcStatus::SUCCESS) {
    return false;
  }
  CHECK(response.isType<Message::kAck>());
  return true;
}

bool SpatialIndex::initReplicatorRpc(const PeerId& to, size_t index,
                                     const DataMap& data) {
  Message request, response;
  proto::FetchResponsibilitiesResponse push_request;
  for (const DataMap::value_type& item : data) {
    proto::AddDataRequest* slot = push_request.add_data();
    slot->set_key(item.first);
    slot->set_value(item.second);
  }
  push_request.set_replicator_index(index);
  request.impose<kInitReplicatorRequest>(push_request);
  if (rpc(to, request, &response) != RpcStatus::SUCCESS) {
    return false;
  }
  CHECK(response.isType<Message::kAck>());
  return true;
}

bool SpatialIndex::appendOnReplicatorRpc(const PeerId& to, size_t index,
                                         const DataMap& data) {
  Message request, response;
  proto::FetchResponsibilitiesResponse push_request;
  for (const DataMap::value_type& item : data) {
    proto::AddDataRequest* slot = push_request.add_data();
    slot->set_key(item.first);
    slot->set_value(item.second);
  }
  push_request.set_replicator_index(index);
  request.impose<kAppendReplicationDataRequest>(push_request);
  if (rpc(to, request, &response) != RpcStatus::SUCCESS) {
    return false;
  }
  CHECK(response.isType<Message::kAck>());
  return true;
}

bool SpatialIndex::fetchFromReplicatorRpc(const PeerId& to, size_t index,
                                          DataMap* data, PeerId* peer) {
  CHECK_NOTNULL(data);
  CHECK_NOTNULL(peer);
  Message request, response;
  proto::FetchReplicationDataRequest data_request;
  data_request.set_replicator_index(index);
  request.impose<kFetchReplicationDataRequest>(data_request);
  if (rpc(to, request, &response) != RpcStatus::SUCCESS) {
    return false;
  }
  CHECK(response.isType<kFetchReplicationDataResponse>());
  proto::FetchResponsibilitiesResponse fetch_response;
  response.extract<kFetchReplicationDataResponse>(&fetch_response);
  for (int i = 0; i < fetch_response.data_size(); ++i) {
    data->emplace(fetch_response.data(i).key(), fetch_response.data(i).value());
  }
  *peer = PeerId(fetch_response.replicator_peer_id());
  return true;
}

void SpatialIndex::localUpdateCallback(const std::string& key,
                                       const std::string& old_value,
                                       const std::string& new_value) {
  SpatialIndexCellData old_data, new_data;
  old_data.ParseFromString(old_value);
  new_data.ParseFromString(new_value);
  common::IdList new_chunks;
  if (new_data.chunkIdSetDiff(old_data, &new_chunks)) {
    for (int i = 0; i < new_data.listeners_size(); ++i) {
      // TODO(tcies) Prune non-responding listeners.
      sendTriggerNotification(PeerId(new_data.listeners(i)), keyToPosition(key),
                              new_chunks);
    }
  }
}

const char SpatialIndex::kTriggerRequest[] =
    "map_api_spatial_index_trigger_request";
MAP_API_PROTO_MESSAGE(SpatialIndex::kTriggerRequest,
                      proto::SpatialIndexTrigger);
void SpatialIndex::sendTriggerNotification(const PeerId& peer,
                                           const size_t position,
                                           const common::IdList& new_chunks) {
  proto::SpatialIndexTrigger trigger_data;
  trigger_data.set_table_name(table_name_);
  trigger_data.set_position(position);
  for (const common::Id& id : new_chunks) {
    id.serialize(trigger_data.add_new_chunks());
  }

  if (peer == PeerId::self()) {
    // Cause trigger on self.
    NetTableManager::instance().getTable(table_name_).handleSpatialIndexTrigger(
        trigger_data);
    return;
  }
  if (!Hub::instance().hasPeer(peer)) {
    LOG(WARNING) << "Spatial index listener " << peer << " not in hub!";
    return;
  }

  Message request, response;
  request.impose<kTriggerRequest>(trigger_data);

  if (!Hub::instance().try_request(peer, &request, &response)) {
    LOG(WARNING) << "Spatial index listener " << peer << " not reachable!";
    return;
  }

  CHECK(response.isOk());
}

} /* namespace map_api */
