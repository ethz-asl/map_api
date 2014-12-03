#ifndef MAP_API_SPATIAL_INDEX_H_
#define MAP_API_SPATIAL_INDEX_H_

#include <sstream>  // NOLINT
#include <string>
#include <unordered_set>
#include <vector>

#include <glog/logging.h>
#include <google/protobuf/repeated_field.h>

#include <map-api/chord-index.h>
#include <map-api/peer-handler.h>

namespace map_api {
class Id;

class SpatialIndex : public ChordIndex {
 public:
  // TODO(tcies) template class on type and dimensions
  struct Range {
    double min, max;
    inline double span() const { return max - min; }
    Range() : min(0), max(0) {}
    Range(double _min, double _max) : min(_min), max(_max) {}  // NOLINT
  };
  class BoundingBox : public std::vector<Range> {
   public:
    BoundingBox() : std::vector<Range>() {}
    explicit BoundingBox(int size) : std::vector<Range>(size) {}
    explicit BoundingBox(const std::initializer_list<Range>& init_list)
        : std::vector<Range>(init_list) {}
    inline std::string debugString() const {
      std::ostringstream ss;
      bool first = true;
      for (Range range : *this) {
        ss << (first ? "" : ",") << range.min << "," << range.max;
        first = false;
      }
      return ss.str();
    }
    inline void serialize(google::protobuf::RepeatedField<double>* field)
        const {
      field->Clear();
      for (const Range& range : *this) {
        field->Add(range.min);
        field->Add(range.max);
      }
    }
    inline void deserialize(
        const google::protobuf::RepeatedField<double>& field) {
      CHECK_EQ(field.size() % 2u, 0u);
      clear();
      for (int i = 0; i < field.size(); i += 2) {
        push_back(Range(field.Get(i), field.Get(i + 1)));
      }
    }
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
  void announceChunk(const Id& chunk_id, const BoundingBox& bounding_box);
  void seekChunks(const BoundingBox& bounding_box,
                  std::unordered_set<Id>* chunk_ids);

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
  void getCellIndices(const BoundingBox& bounding_box,
                      std::vector<size_t>* indices) const;
  inline size_t coefficientOf(size_t dimension, double value) const;
  /**
   * TODO(tcies) template ChordIndex on key type?
   */
  static inline std::string typeHack(size_t cell_index);

  /**
   * TODO(tcies) the below is basically a copy of NetTableIndex AND
   * ChordIndexTest
   */
  bool rpc(const PeerId& to, const Message& request, Message* response);

  virtual bool getClosestPrecedingFingerRpc(const PeerId& to, const Key& key,
                                            PeerId* closest_preceding)
      final override;
  virtual bool getSuccessorRpc(const PeerId& to,
                               PeerId* predecessor) final override;
  virtual bool getPredecessorRpc(const PeerId& to,
                                 PeerId* predecessor) final override;
  virtual bool lockRpc(const PeerId& to) final override;
  virtual bool unlockRpc(const PeerId& to) final override;
  virtual bool notifyRpc(const PeerId& to,
                         const PeerId& subject) final override;
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

  std::string table_name_;
  BoundingBox bounds_;
  std::vector<size_t> subdivision_;
  PeerHandler peers_;
};

} /* namespace map_api */

#endif  // MAP_API_SPATIAL_INDEX_H_
