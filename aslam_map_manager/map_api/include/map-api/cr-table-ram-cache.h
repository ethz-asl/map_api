#ifndef MAP_API_CR_TABLE_RAM_CACHE_H_
#define MAP_API_CR_TABLE_RAM_CACHE_H_

namespace map_api {

/**
 * TODO(tcies) make this a proper RAM cache (maps, not RAM sqlite) and create
 * a CRTableDiskCache for disk SQLite
 */
class CRTableRAMCache final : public CRTable {
 public:
  virtual ~CRTableRAMCache();
 private:
  virtual bool initCRDerived() override;
  virtual void defineDefaultFieldsCRDerived() override;
  virtual void ensureDefaulFieldsCRDerived(Revision* query) const override;
  virtual bool insertCRDerived(Revision* query) override;
  virtual int findByRevisionCRDerived(
      const std::string& key, const Revision& valueHolder, const Time& time,
      std::unordered_map<Id, std::shared_ptr<Revision> >* dest) override;

  /**
   * Parse and execute SQL query necessary to create the table schema in the
   * database.
   */
  bool createQuery();

  bool isSqlSafe(const std::string& string) const;

  std::weak_ptr<Poco::Data::Session> session_;
  /**
   * The PocoToProto class serves as intermediate between Poco and Protobuf:
   * Because Protobuf doesn't support pointers to numeric fields and Poco Data
   * can't handle blobs saved as std::strings (which is used in Protobuf),
   * this intermediate data structure is required to pass data from Poco::Data
   * to our protobuf objects.
   */
  class PocoToProto {
   public:
    /**
     * Associating with Table interface object to get template
     */
    PocoToProto(const CRTable& table);
    /**
     * To be inserted between "SELECT" and "FROM": Bind database outputs to
     * own structure.
     */
    void into(Poco::Data::Statement& statement);
    /**
     * Applies the data obtained after statement execution onto a vector of
     * Protos. Returns the element count.
     */
    int toProto(std::vector<std::shared_ptr<Revision> >* dest);
   private:
    const CRTable& table_;
    /**
     * Maps where the data is store intermediately
     */
    std::map<std::string, std::vector<double> > doubles_;
    std::map<std::string, std::vector<int32_t> > ints_;
    std::map<std::string, std::vector<int64_t> > longs_;
    std::map<std::string, std::vector<Poco::Data::BLOB> > blobs_;
    std::map<std::string, std::vector<std::string> > strings_;
    std::map<std::string, std::vector<std::string> > hashes_;
  };
};

} /* namespace map_api */

#endif /* MAP_API_CR_TABLE_RAM_CACHE_H_ */
