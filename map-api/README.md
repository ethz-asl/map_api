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

### Accessing items in replicated chunks

*Note: If you are interested in developing dmap applications that are easy to
use, it's a must to check out the chapter on 
[application-defined views](#application-defined-views) after this one.
They significantly simplify the interface presented here but require additional
setup.*

To guarantee consistency and consensus we enforce access to dmap data through
a specific interface we call a *Transaction*. If you are familiar with git, you
should be able to get used to Transactions fairly quickly. Here is how to use
them, assuming we have defined a table as above:

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

### Gaining access to chunks created by other peers

## Advanced concepts

### Application-defined views

### Triggers

### Spatial indices

### Chunk dependencies between NetTables

### Workspaces

### Auto-merging policies
