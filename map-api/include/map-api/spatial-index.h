#ifndef MAP_API_SPATIAL_INDEX_H_
#define MAP_API_SPATIAL_INDEX_H_

#include <sstream>  // NOLINT
#include <string>
#include <unordered_set>
#include <vector>

#include <Eigen/Dense>
#include <glog/logging.h>
#include <google/protobuf/repeated_field.h>
#include <gtest/gtest_prod.h>

#include "map-api/chord-index.h"
#include "map-api/peer-handler.h"
#include "map-api/spatial-index-cell-data.h"

namespace common {
class Id;
}  // namespace common

namespace map_api {
namespace proto {
class SpatialIndexTrigger;
}  // namespace proto

class SpatialIndex : public ChordIndex {
 public:
  // TODO(tcies) template class on type and dimensions
  struct Range {
    double min, max;
    inline double span() const { return max - min; }
    Range() : min(0), max(0) {}
    Range(double _min, double _max) : min(_min), max(_max) {}  // NOLINT
  };
  // TODO(tcies) replace with Eigen::AlignedBox
  class BoundingBox : public std::vector<Range> {
   public:
    BoundingBox();
    explicit BoundingBox(int size);
    explicit BoundingBox(const std::initializer_list<Range>& init_list);
    BoundingBox(const Eigen::Vector3d& min, const Eigen::Vector3d& max);
    std::string debugString() const;
    void serialize(google::protobuf::RepeatedField<double>* field) const;
    void deserialize(const google::protobuf::RepeatedField<double>& field);
  };

  virtual ~SpatialIndex();
  void handleRoutedRequest(const Message& routed_request, Message* response);

  /**
   * Overriding create() to automatically create all cells at index creation
   * time.
   */
  void create();

  /**
   * Without guarantee of consistency - the only thing that is (needed to be)
   * guaranteed is that if at least one peer holds a chunk, at least one peer
   * will be registered in the index.
   */
  void announceChunk(const common::Id& chunk_id,
                     const BoundingBox& bounding_box);
  void seekChunks(const BoundingBox& bounding_box, common::IdSet* chunk_ids);
  void listenToSpace(const BoundingBox& bounding_box);

  typedef std::function<void(const common::Id& id)> TriggerCallback;

  // Also used as iterator for range-based for loops.
  class Cell {
   public:
    Cell(size_t position_1d, SpatialIndex* index);

    void getDimensions(Eigen::AlignedBox3d* result);
    std::string chordKey() const;

    void announceAsListener();
    void getListeners(PeerIdSet* result);

    // Iterator interface.
    Cell& operator++();
    // This is a bit strange, but we want to fit into the range-loop interface.
    inline Cell& operator*() { return *this; }
    bool operator!=(const Cell& other);

    class Accessor {
     public:
      explicit Accessor(Cell* cell);
      Accessor(Cell* cell, size_t timeout_ms);
      ~Accessor();
      inline SpatialIndexCellData& get() {
        dirty_ = true;
        return data_;
      }
      inline const SpatialIndexCellData& get() const { return data_; }

     private:
      Cell& cell_;
      SpatialIndexCellData data_;
      bool dirty_;
    };

    inline Accessor accessor() { return Accessor(this); }
    inline const Accessor constAccessor() { return Accessor(this); }
    inline const Accessor constPatientAccessor(size_t timeout_ms) {
      return Accessor(this, timeout_ms);
    }

   private:
    // x is most significant, z is least significant.
    size_t position_1d_;
    SpatialIndex* index_;
  };

  size_t size() const;
  Cell begin();
  Cell end();

  static const char kRoutedChordRequest[];
  static const char kPeerResponse[];
  static const char kGetClosestPrecedingFingerRequest[];
  static const char kGetSuccessorRequest[];
  static const char kGetPredecessorRequest[];
  static const char kLockRequest[];
  static const char kUnlockRequest[];
  static const char kNotifyRequest[];
  static const char kReplaceRequest[];
  static const char kAddDataRequest[];
  static const char kRetrieveDataRequest[];
  static const char kRetrieveDataResponse[];
  static const char kFetchResponsibilitiesRequest[];
  static const char kFetchResponsibilitiesResponse[];
  static const char kPushResponsibilitiesRequest[];
  static const char kInitReplicatorRequest[];
  static const char kAppendReplicationDataRequest[];
  static const char kTriggerRequest[];

 private:
  /**
   * Life cycle managed by NetTable!
   */
  SpatialIndex(const std::string& table_name, const BoundingBox& bounds,
               const std::vector<size_t>& subdivision);
  SpatialIndex(const SpatialIndex&) = delete;
  SpatialIndex& operator=(const SpatialIndex&) = delete;
  friend class NetTable;

  /**
   * Given a bounding box, identifies the indices of the overlapping cells.
   */
  void getCellsInBoundingBox(const BoundingBox& bounding_box,
                             std::vector<Cell>* cells);
  inline size_t coefficientOf(size_t dimension, double value) const;

  static inline std::string positionToKey(size_t cell_index);
  static inline size_t keyToPosition(const std::string& key);

  /**
   * TODO(tcies) the below is basically a copy of NetTableIndex AND
   * ChordIndexTest
   */
  ChordIndex::RpcStatus rpc(const PeerId& to, const Message& request,
                            Message* response);

  virtual bool getClosestPrecedingFingerRpc(const PeerId& to, const Key& key,
                                            PeerId* closest_preceding)
      final override;
  virtual bool getSuccessorRpc(const PeerId& to,
                               PeerId* predecessor) final override;
  virtual bool getPredecessorRpc(const PeerId& to,
                                 PeerId* predecessor) final override;
  virtual ChordIndex::RpcStatus lockRpc(const PeerId& to) final override;
  virtual ChordIndex::RpcStatus unlockRpc(const PeerId& to) final override;
  virtual bool notifyRpc(const PeerId& to, const PeerId& subject,
                         proto::NotifySender sender_type) final override;
  virtual bool replaceRpc(const PeerId& to, const PeerId& old_peer,
                          const PeerId& new_peer) final override;
  virtual bool addDataRpc(const PeerId& to, const std::string& key,
                          const std::string& value) final override;
  virtual bool retrieveDataRpc(const PeerId& to, const std::string& key,
                               std::string* value) final override;
  virtual bool fetchResponsibilitiesRpc(
      const PeerId& to, DataMap* responsibilities) final override;
  virtual bool pushResponsibilitiesRpc(
      const PeerId& to, const DataMap& responsibilities) final override;
  virtual bool initReplicatorRpc(const PeerId& to, size_t index,
                                 const DataMap& data) final override;
  virtual bool appendOnReplicatorRpc(const PeerId& to, size_t index,
                                     const DataMap& data) final override;

  virtual void localUpdateCallback(const std::string& key,
                                   const std::string& old_value,
                                   const std::string& new_value);

  void sendTriggerNotification(const PeerId& peer, const size_t position,
                               const common::IdList& new_chunks);

  std::string table_name_;
  BoundingBox bounds_;
  std::vector<size_t> subdivision_;
  PeerHandler peers_;
};

} /* namespace map_api */

#endif  // MAP_API_SPATIAL_INDEX_H_
