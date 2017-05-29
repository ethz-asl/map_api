# Map API

If you use Map API in your academic work, please cite:
```tex
@inproceedings{cieslewski2015mapapi,
  title={Map API - Scalable Decentralized Map Building for Robots},
  author={Cieslewski, Titus and Lynen, Simon and Dymczyk, Marcin and Magnenat, St\'{e}phane and Siegwart, Roland},
  booktitle={Robotics and Automation (ICRA), 2015 IEEE International Conference on},
  year={2015}
}
```

## Installation

Requires ROS for building. Additionally, install [catkin tools](http://catkin-tools.readthedocs.org/en/latest/installing.html) and rosinstall if you didn't do so before:

    sudo apt-get install python-catkin-tools python-rosinstall

Create a new catkin workspace if needed. Note that the dependencies unfortunately require `--merge-devel` (replace `indigo` with your ROS distribution):

    mkdir -p my_ws/src
    cd my_ws
    catkin config --init --mkdirs --extend /opt/ros/indigo \
      --merge-devel --cmake-args -DCMAKE_BUILD_TYPE=Release
    
Clone Map API:

    cd src
    git clone git@github.com:ethz-asl/map_api.git
    
Clone dependencies:

    wstool init
    wstool merge map_api/dependencies.rosinstall
    wstool update

Build:

    catkin build

## Overview

This manual is aimed at the reader who is interested in 
*implementing or extending* a library that uses Map API as a framework for sharing
data between agents.

If you are just using such a library, all that might be useful to you in this
document can be found in 
[this section](#things-to-consider-when-using-map-api-to-modify-shared-map-data).

If you want to understand how Map API works, this manual might be an interesting
starting point before reading the paper.

## How to use Map API

Map API is a fully decentralized data distribution framework that has been
developed in order to allow multiple robots to share mapping data as they
collaboratively map an environment.

In a nutshell, Map API is to a multi-agent system what (git + github) is to
code collaborators, except that it's fully peer-to-peer and doesn't force agents
to replicate data they are not interested in (and doesn't have branches or
commit messages).
It has some additional features useful for multi-agent mapping like 3d querying
and event callbacks.

For sake of example, let's assume you are writing a multi-agent mapping 
application and have a pose-graph class like this:

```c++
class PoseGraph {
 public:
  void buildFromEstimatorOutput();
  void optimize();
  void useForLocalization();
  
 private:
  std::unordered_map<VertexId, Vertex> vertices_;
  std::unordered_map<EdgeId, Edge> edges_;
}
```

Then Map API allows you to go from single-agent code like this:

```c++
PoseGraph pose_graph;
pose_graph.buildFromEstimatorOutput();
pose_graph.optimize();
// later...
pose_graph.useForLocalization();
```

to multi-agent code like this:

```c++
// On agent 1:
PoseGraphView pose_graph_view;
pose_graph_view.pose_graph().buildFromEstimatorOutput()
pose_graph_view.commit();

// On agent 2:
fetchDataFromOtherAgents();
PoseGraphView pose_graph_view;
pose_graph_view.pose_graph().optimize()
pose_graph_view.commit();

// On agent 3:
fetchDataFromOtherAgents();
PoseGraphView pose_graph_view;
pose_graph_view.pose_graph().useForLocalization()
```

or even using callbacks for asynchronous operation, like this:

```c++
// Agent 1: Same as above.

// Agent 2:
createCallBackForWhenNewDataArrives([](){
  PoseGraphView pose_graph_view;
  pose_graph_view.pose_graph().optimize();
  pose_graph_view.commit();
});

// Agent 3:
createCallBackForWhenDataIsModified([](){
  PoseGraphView pose_graph_view;
  pose_graph_view.pose_graph().useOptimizedPartForLocalization();
});
```

This can then scale to arbitrary numbers of peers (independently whether those
peers are pose-graph producers or consumers). All the data is available to all
the agents (there are ways to discover data based on location), even though no
agent is forced to keep data it doesn't want to (thus technically enabling
distribution of very-large scale maps on many peers). Finally, all the data
is synchronized in a fashion very similar to git and all of this works 
without a central entity.

Note the new class `PoseGraphView`. This is a data inteface that allows
interaction with a consistent snapshot of the data, even though the
"latest version" of the data might be updated by other agents in the meantime.
This is equivalent to how a commit hash defines a "view" of the data in a git
repository.

### Data structure

We provide the following UML diagram for reference throughout this documentation:

![uml](map-api/doc/uml.png)

In order to make use of Map API, your application should express its data, let's
call it AppData, in terms of **singleton** associative containers with ids as
keys (singleton means that there is a only a single such container per process):

```c++
class AppData {
  // ...
  std::unordered_map<IdType1, DataType1> container_1_;
  std::unordered_map<IdType2, DataType2> container_2_;
  // etc ...
};
```

However, instead of using unordered maps, you will need to use 
`common::MappedContainerBase`s and instead of using arbitrary id types, you will
need to use id types derived from `common::Id`:

```c++
#include <multiagent-mapping-common/mapped-container-base.h>
#include <multiagent-mapping-common/unique-id.h>

UNIQUE_ID_DEFINE_ID(IdType1);
UNIQUE_ID_DEFINE_ID(IdType2);

class AppData {
  // ...
  std::unique_ptr<map_api_common::MappedContainerBase<IdType1, DataType1>> container_1_;
  std::unique_ptr<map_api_common::MappedContainerBase<IdType2, DataType2>> container_2_;
  // ...
};

// Outside of any namespace:
UNIQUE_ID_DEFINE_ID_HASH(IdType1);
UNIQUE_ID_DEFINE_ID_HASH(IdType2);
```

While you can techincally just use the `multiagent-mapping-common` base Id 
class, `common::Id`, we strongly recommend using
strongly typed ids as provided by the above macros, as this will make it harder
to make silly errors like passing the wrong id type to functions.

At this point, you can continue to have a Map API-free version of your basic app,
by instantiating the `common::MappedContainerBase`s with 
`common::HashMapContainer`s, which are defined in the same header. The latter
are essentially equivalent to `std::unordered_map`. This allows you to easily 
switch between using and not using Map API, the latter of course being faster in a
single-agent setting.

Now you will first need to [define how your data types can be translated into 
the internal data representation of Map API](#Defining-type-to-revision-translations).
Then, we recommend you 
[define a view class](#defining-view-classes) similar to the example in the 
intro that will allow you to check out and commit the distributed map state.

### Defining type-to-revision translations

*Revisions* are the internal representation of individual data items in Map API.
They are called *Revisions* because data items are subject to change - a
revision captures the state of a data item at a given time. The history of an
item is represented by a sequence of revisions associated to a time, where the
times are times when changes occurred and the revision corresponds to the new 
state at that time.

Consequently, you will need to define how the types of the data you want to
distribute using Map API can be translated into these revisions.
Depending on your type, you can choose to translate it into a *protobuf* or a
*composite* revision. *Protobuf* revisions contain a single serialization string
that is generated by 
[Google Protocol Buffers](https://developers.google.com/protocol-buffers/) from
an intermediate protocol buffer representation. A protocol buffer representation
of a type is significantly more comfortable to implement than implementing a
serialization directly.
Alternatively, you can express your type as a *composite revisions* composed of
the types listed at the bottom of `map-api/revision-inl.h`.
We recommend to translate into protobuf revisions by default, and use
composite revisions for special cases only.

First, you will need to specialize 
`map_api::objectFromRevision(const map_api::Revision&, ObjectType*)`
and
`map_api::objectToRevision(const ObjectType&, map_api::Revision*)` for your type. 

If you go for protobuf revisions you can simply use the corresponding macro from 
`map-api/app-templates.h`. Consult the macro expansion for the expected Protocol
Buffer serialization and deserialization signatures.

Otherwise, if you prefer composite revisions, your specializations should look
similar to the following example:

```c++
enum Fields {
  kIndex, kName, kValue
};

template<>
map_api::objectFromRevision(const map_api::Revision& revision, DataType* object) {
  revision.get(kIndex, &object->index);
  revision.get(kName, &object->name);
  revision.get(kValue, &object->value);
}

template<>
map_api::objectToRevision(const DataType& object, map_api::Revision* revision) {
  revision->set(kIndex, object.index);
  revision->set(kName, object.name);
  revision->set(kValue, object.value);
}
```

#### Defining NetTables

For each data type, Map API has a container called `NetTable` which holds the
history of all items of that type. In order to use these containers, you will
need to initialize them with a unique name (that will be used for addressing
data packets, so that the correct data arrives at the correct container on a
remote peer) and a specification of the revision format (used for verifying that
the correct data has arrived at a given container).

For protobuf revisions, this is as simple as:

```c++
// TODO(tcies) un-sharedptr, static functions of NetTableManager?
std::shared_ptr<map_api::TableDescriptor> descriptor<ProtobufType>("table_name");
map_api::NetTable* table = map_api::NetTableManager::instance().addTable(descriptor);
```

For composite revisions, you need to declare the individual fields:
```c++
std::shared_ptr<map_api::TableDescriptor> descriptor;
descriptor->setName("table_name");
descriptor->addField<std::string>(kName);
descriptor->addField<uint64_t>(kIndex);
descriptor->addField<double>(kValue);
map_api::NetTable* table = map_api::NetTableManager::instance().addTable(descriptor);
```

As you add a table through the NetTableManager, Map API verifies that your
table description corresponds to table descriptions of the same table (
identified by the name) on other agents.

### Defining view classes

Access to the Map API data structure is provided through so-called transactions.
This access protocol guarantees two things:

1. It ensures that all the data, as seen by the agent, is consistent, even if
the data can change at any time due to activity by other agents. This is
achieved by defining a view time: Changes applied to the data after this time
will not be seen by the transaction instance.
2. It ensures that changes applied by the agent will either be seen by other
agents all at the same time or not at all.

This protocol is very similar to git, where you check out a commit and will not
see changes applied by your colleagues in the meantime until you check out a
newer commit.

In order to integrate Map API into your application with the least effort, we 
recommend defining a class analogous to the following `AppDataView`:

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
  map_api::Transaction transaction_;
};
```

By instantiating the `AppData` `common::MappedContainerBase`s with 
`map_api::ThreadsafeCache`s, the `data_` member will now represent the state of
the shared data as it is at construction time of the `transaction_` member.

### Things to consider when using Map API to modify shared map data

#### Generally

* Opening a transaction does not incur network traffic, and is generally not 
expensive.
* `MappedContainerBase::getMutable()` should only be used in order to modify
data, since every item thus accessed will be marked as modified and consequently
sent over the network at commit-time.
* Transactions can be committed multiple times. This can be used to keep the
shared data up to date when necessary. However, the view time will remain the
same.

#### Commit failures

A commit can fail if items that you update or remove have also been updated or
removed by other peers during the scope of your transaction. If this is the
case, you are in trouble. While we exhibit an interface to "manually" merge
conflicts, which we won't document here, we recommend to avoid conflicts
altogether. This can be achieved through careful design and 
[automated merging](#auto-merging-policies). You may also consider simply
accepting commit failures and discarding changes, depending on your application.

### Getting access to data created by other agents

Up until now we have made the assumption that the data we are intersted in is
present on the agent at hand. This, however, is per default not the case for
data that has been created by other agents. In order to scale, a design decision
that has been made for Map API is that an agent shall only keep the data it is
explicitly interested in.

To that end, Map API organizes data in so-called chunks. At this point in the
documentation, there is a choice for you. To paraphrase Morpheus:

*This is your last chance. After this, there is no turning back. You don't learn
about chunks -- the story ends, you wake up in your bed and can use Map API for
small-scale applications. You learn about chunks -- you stay in Wonderland and I
show you how Map API was designed to scale to arbitrary size.*

#### If you choose not to learn about chunks:

All you need to know is how to grab all the data from the network:

```c++
table->replicateAllChunksInTheNetwork(chunk_id);  // TODO(tcies) implement.
```

#### If you choose to learn about chunks:

Chunks are groups of data from the same container that an agent can either have
access to entirely, or not at all. Each chunk runs a consensus protocol which
ensures that the most up-to-date shared state of the contained data is
replicated on all agents. The reason for not sharing items individually
is that there is overhead involved in the consensus protocol, and having more
items in the same consensus group is less expensive.

Hence, the chunk size represents a middle ground between reducing the consensus
protocol overhead and not forcing agents to maintain data that they are not
interested in. When using a `map_api::ThreadsafeCache`, chunks are created
automatically as new data is inserted: Chunks are filled up to a certain size,
at which a new chunk is initialized for subsequent insertions.

Each chunk has an id and if an id of a chunk is known it can be retrieved as
follows:

```c++
table->getChunk(chunk_id);
```

The problem is that for many use cases, the chunk-ids are not known in advance.
Hence, Map API provides interfaces to 
[listen to new chunks from the entire network or specific peers](#triggers), 
as well as to [perform queries in 3d space](#spatial-indices).

Also, note that Map API provides an interface to
[track data dependencies between tables](#data-dependencies-between-nettables).

## Advanced concepts

### Triggers

Map API allows you to attach callbacks to network events:

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

We have seen above how to access chunks from other peers by id. Map API furthermore
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

### Data dependencies between NetTables

In many cases, you might have dependencies between items from different tables.
For instance, if some type A points to several items of type B, you might want
to add a mechanism that ensures that the items pointed to are available,
especially since Map API does not allow search of individual items in the network
for scalability reasons. Such dependencies can be comfortably added in Map API
using *Chunk tracking*. For the above example, we would say that items of A 
track chunks of B: A is the *tracker type*, B is the *trackee type*.

If the dependency can be resolved only from information in a B item:
```c++
// In the cc file of your setup:
namespace map_api {
template <>
TrackerIdType determineTracker<TrackeeType, TrackerType, TrackerIdType>(
    const TrackeeType& trackee) {
  // ...
  return tracker_id;
}
template <>
NetTable* tableForType<typename TrackerType>() {
  return tracker_table;
}
}  // namespace map_api

// During setup:
trackee_table->
    pushNewChunkIdsToTracker<TrackeeType, TrackerType, TrackerIdType>();
```

If more transaction-time information is needed:
```c++
// During setup:
trackee_table->
    pushNewChunkIdsToTracker(tracker_table);

// Before committing the transaction:
transaction.overrideTrackerIdentificationMethod(trackee_table, tracker_table,
    [&identification_helpers](const Revision& trackee_revision){
  // ...
  return tracker_id;
});
```

Either way, the transaction will now push chunk ids of newly created items in
B to the corresponding items of A before committing the transaction. Note that
this will update the A items even if they haven't been modified explicitly
during the transaction.

Tracked chunks can then be fetched, e.g. at the beginning of a transaction:

```c++
transaction.fetchAllChunksTrackedByItemsInTable(tracker_table);
// or, for individual items:
transaction.fetchAllChunksTrackedBy(tracker_id, tracker_table);
```

### Auto-merging policies

As mentioned above, commit conflicts should be generally avoided by design.
However, for situations where they can't be avoided, it's possible to specify
auto-merging policies. 

For instance, if an item contains some list that peers can add list-items to, a
commit conflict would ensue if two peers would add different items at the same
time. However, it could make sense in an application to not treat this as a
conflict, but to instead just consolidate both additions (in fact, this is
automatically done for [chunk tracking](#Data-dependencies-between-NetTables)).

For this example, adding such a merge policy would be achieved using the
following code (assuming `objectFromRevision()`, `objectToRevision()`, 
`getObjectDiff()` and `applyObjectDiff()` are specialized for the item class in
question):

```c++
// In the setup cc file:
bool autoMergeList(const ItemDiff& conflicting_diff, ItemDiff* diff_at_hand) {
  CHECK_NOTNULL(diff_at_hand);
  
  if (!conflicting_diff.onlyListHasChanged() || 
      !diff_at_hand->onlyListHasChanged()) {
    return false;
  }
  
  const ListDiff conflicting_list_diff = conflicting_diff.getListDiff();
  ListDiff list_diff_at_hand = diff_at_hand->getListDiff();
  
  // Make auto-merge fail if removals are present.
  if (conflicting_list_diff.containsRemovals() || 
      list_diff_at_hand.containsRemovals()) {
    return false;
  }
  
  list_diff_at_hand.consolidate(conflicting_list_diff);
  diff_at_hand->setListDiff(list_diff_at_hand);
  return true;
}

// During setup:
// TODO(tcies) adapt interface to template on object type.
table->addAutoMergePolicy(autoMergeList);
```

Note that several policies can be specified per table. Auto-merging will try
them all until one succeeds.
