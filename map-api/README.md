# dmap

If you use dmap in your academic work, please cite:
```tex
@inproceedings{cieslewski2015mapapi,
  title={Map API - Scalable Decentralized Map Building for Robots},
  author={Cieslewski, Titus and Lynen, Simon and Dymczyk, Marcin and Magnenat, St\'{e}phane and Siegwart, Roland},
  booktitle={Robotics and Automation (ICRA), 2015 IEEE International Conference on},
  year={2015}
}
```

## How to use dmap

Essentially, dmap provides a collection of distributed containers called 
*NetTables*, where each NetTable contains several id-referenced serialized items
of a user-defined type. These items can be shared with remote processes in a
decentralized version control scheme.

The hierarchical data structure is as follows, where each object contains
multiple objects of the class below:

```bash
NetTableManager  # Singleton. Manages access to and creation of NetTables.
  NetTable       # Explained above. A NetTable can only store items of one type.
    Chunk        # A collection of items that are shared together.
      Item       # An item representation that contains its history.
        Revision # A version of the item within its history.
```

Chunks are collections of items that are always shared together. A peer can
either have access to a chunk by replicating it entirely, or not have access to
it at all.

ADD A GRAPHIC REPRESENTING MULTIPLE PEERS

We will now discuss how to define NetTables for custom types. We will then
discuss the interface by which items that are available in replicated chunks are
accessed. The interface guarantees:

* Consensus among peers on the state of the chunk items.
* Minimal block times on accesses through optimistic concurrency control.
* A consistent view of items across NetTables, even though items can be modified at all times.

At the same time the interface hides all the logic and protocol that are 
necessary to make these guarantees.

Finally, we will discuss how a peer can gain access to chunks that it doesn't
currently replicate.

### Defining NetTables

To define dmap NetTables for your types, you need to specify how your type can
be serialized into a dmap Revision. While there are many ways to do this, we
recommend to specify a Google Protocol Buffer (protobuf) serialization. 
With a protobuf type, you can simply define a table with the following code:

```c++
// TODO(tcies) un-sharedptr, static functions of NetTableManager?
// TODO(tcies) one-liner for single-field tables?
std::shared_ptr<dmap::TableDescriptor> descriptor;
descriptor->setName("table_name");
descriptor->addField<ProtobufType>(0);
dmap::NetTable* table = dmap::NetTableManager::instance().addTable(descriptor);
```

Alternatively, you can define types composed of multiple sub-types:
```c++
enum Fields {
  kName, kIndex, kValue
}
descriptor->addField<std::string>(kName);
descriptor->addField<uint64_t>(kIndex);
descriptor->addField<double>(kValue);
```

As you add a table through the NetTableManager, dmap verifies that your
table description corresponds to table descriptions of the same table (
identified by the name) on other peers. To scale, you should pick names that
contain your package name / workspace.

### Creating chunks or getting access to chunks created by other peers

You can create chunks:
```c++
common::Id chunk_id;  // TODO(tcies) Type strongly?
dmap::ChunkBase* chunk = table->newChunk(&chunk_id);  // Id is optional.
```
Access chunks by id (starting to replicate them if not doing so yet):
```c++
dmap::ChunkBase* chunk = table->getChunk(chunk_id);
```
Or simply access all chunks available on all peers:
```c++
table->replicateAllChunksInTheNetwork(chunk_id);  // TODO(tcies) implement.
```

Since the latter doesn't scale well, we recommend to use it for prototyping or
small-scale applications only. For large-scale applications, we recommend using
[table triggers](#triggers) or [spatial indices](#spatial-indices).

Your application should ensure that chunks that are created are neither too
big (takes a long time to transfer) nor too small (too much overhead for
managing too many chunks). As a rule of thumb, a typical use case should
involve only a handful of chunks per table.

### Accessing and modifying items in replicated chunks

*Note: If you are interested in developing dmap applications that are easy to
use, it's a must to check out the chapter on 
[application-defined views](#application-defined-views) after this one.
They significantly simplify the interface presented here but require additional
setup.*

To guarantee consistency and consensus we enforce access to dmap data through
a specific interface we call a *Transaction*. If you are familiar with git, you
should be able to get used to Transactions fairly quickly. Here is how to use
them, assuming you have defined a table and gained access to chunks as above:

```c++
dmap::Transaction transaction;

// Insert a Protocol Buffer:
std::shared_ptr<Revision> insert_revision = table->getTemplate();
revision->set(0, protobuf);
IdType insert_id = transaction.insert<IdType>(table, chunk, revision);  // TODO(tcies) fix interface

// Read protobuf with id read_id:
std::shared_ptr<const Revision> read_revision = 
    transaction.getById(read_id, table);
ProtoType protobuf;
read_revision->get(0, &protobuf);
// TODO(tcies) We could so move away from shared pointers...

// Update an item:
std::shared_ptr<Revision> update_revision = read_revision.copyForWrite();
update_revision->set(0, updated_protobuf);
transaction.update(table, update_revision);

// Remove an item:
std::shared_ptr<Revision> remove_revision = read_revision.copyForWrite();
transaction.remove(table, remove_revision);
// Yes this is all very ugly.
```

Insertions, updates and removals don't get applied immediately; instead, you
need to commit your changes:

```c++
bool success = transaction.commit();
```

A commit can fail if items that you update or remove have also been updated or
removed by other peers during the scope of your transaction. If this is the
case, you are in trouble. While we exhibit an interface to "manually" merge
conflicts, which we won't document here, we recommend to avoid conflicts
altogether. This can be achieved through careful design and 
[automated merging](#auto-merging-policies). You may also consider simply
accepting commit failures and discarding changes, depending on your application.

Note that `insert()` is the only function which requires knowledge about chunks.
The other functions browse all available chunks for the item in question. You
can indeed abstract away chunks by using a `ChunkManager` for insertions. This
class creates a new chunk for insertion whenever the size of the previous chunk
exceeds a certain threshold.

#### Notes on thread-safety

Access to a single transaction is not thread-safe, you can however use
multiple transaction within your application to make use of the dmap protocol
within your application, just as if each transaction were owned by a different
peer. Special care should be taken when getting access to new chunks: If this
happens while a transaction is active, this could cause inconsistencies. We
might ensure that each transaction has a fixed set of chunks it sees in future
versions of dmap.

## Advanced concepts

### Application-defined views

Dmap data access and modification can be abstraced down to only committing with
application-defined views. The requirement for this is that you can represent
data of your application in instances of `common::MappedContainerBase`
from `multiagent-mapping-common/mapped-container-base.h`:

```c++
class AppData {
  // ...
  std::unique_ptr<common::MappedContainerBase<IdType1, DataType1>> container_1_;
  std::unique_ptr<common::MappedContainerBase<IdType2, DataType2>> container_2_;
  // ...
};
```

Where `IdTypeX` is a strongly typed id type defined with `UNIQUE_ID_DEFINE_ID`
from `multiagent-mapping-common/unique-id.h`. Note that you will also need to
define the `std::hash` specialization using `UNIQUE_ID_DEFINE_ID_HASH`.

Also note that at this point, your application does not yet depend on dmap.
Indeed, you can create a non-distributed version of your app that will run
faster locally by instantiating the mapped containers with 
`common::HashMapContainer`, which is an interface to `std::unordered_map`.

In order to "go distributed", you can now consolidate `AppData` and 
`dmap::Transaction` using `Transaction::createCache`. We recommend doing this
in a view class as follows:

```c++
class AppDataView {
 public:
  AppDataView() {
    data_.container_1_.swap(
        transaction_.createCache<IdType1, DataType1>(table_1));
    data_.container_2_.swap(
        transaction_.createCache<IdType2, DataType2>(table_2));
    // ...
  }
  
  AppData& data() {
    return data_;
  }
  
  bool commitChanges() {
    return transaction_.commit();
  }
  
 private:
  AppData data_;
  dmap::Transaction transaction_;
};
```

For this to work, you will need to specialize 
`dmap::objectFromRevision(const dmap::Revision&, ObjectType*)`
and
`dmap::objectToRevision(const ObjectType&, dmap::Revision*)`. You can find
helpful macros for doing that in `dmap/app-templates.h`.

Once you have set that up, access and modification reduces to:

```c++
AppDataView view;

// Insert a data item (note: no need to serialize to protobuf explicitly!):
IdType insert_id = view.data().container_.insert(some_object);

// Read:
SomeType some_result = view.data().container_.get(read_id).someConstOperation();

// Update an item:
view.data().container_.getMutable(update_id).someNonConstOperation();

// Remove an item:
view.data().container_.erase(remove_id);
```

And finally, if needed:

```c++
bool success = view.commitChanges();
```

### Triggers

Dmap allows you to attach callbacks to network events:

```c++
// Triggered after a remote commit:
chunk->attachTrigger(commit_callback);
table->attachTriggerToCurrentAndFutureChunks(commit_callback);

// Triggered at chunk aquisition:
table->attachCallbackToChunkAcquisition(new_chunk_callback);

// Request a specific peer to share all its chunks of a table:
table->listenToChunksFromPeer(peer_id);

// Subscribe to all new chunks from a table:
NetTableManager::listenToPeersJoiningTable(table_name);
```

Evidently, the callbacks will only be triggered for events after the callbacks
have been specified.

### Spatial indices

We have seen above how to access chunks from other peers by id. Dmap furthermore
provides an interface to access chunks using a bounding box query in 3D space.
A spatial index can be initiated for a table by extending the `TableDescriptor`
at [table definition](#Defining-NetTables) time:

```c++
// TODO(tcies) adapt interface to doc
std::vector<size_t> cell_dimensions_m({10u, 10u, 10u});
descriptor->setSpatialIndex(cell_dimensions);
```

Chunks can be associated with a bounding box using 
`NetTable::registerChunkInSpace()` or `NetTable::registerChunkOfItemInSpace()`.
Then, they can be fetched from any peer with 
`NetTable::getChunksInBoundingBox()`. Note that this assumes consensus on a
global frame of reference for the bounding box coordinates.

### Chunk dependencies between NetTables



### Workspaces

### Auto-merging policies
