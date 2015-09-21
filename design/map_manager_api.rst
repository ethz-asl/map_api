:Info: A map manager API for 3D mapping, navigation and scene interpretation
:Authors: - Stéphane Magnenat <stephane@magnenat.net> (author / proposer)
          - Simon Lynen <simon.lynen@gmail.com> (maintainer)
:Date: 2014-04-09

=======================================================================
 A map manager API for 3D mapping, navigation and scene interpretation
=======================================================================

Rationale
=========

A modern autonomous robotic system is composed of many software building blocks (SLAM, path planning, scene interpretation), most of them depending on some form of localisation and mapping.
Therefore, there is a need for an API that allows these different elements to communicate, with as little interdependency as possible.
This document describes such an API, and discusses its implementation, based on the concept of a transactional map database.
It aims at being as generic as possible and agnostic with respect to the implementation framework.
It is designed to allow for a fully distributed back-end, while abstracting it in such a way that API users do not need to care about it.

Framework requirements
======================

The implementation framework shall allow modularity based on the concept of components.
It shall provide both message passing (called *message* in the rest of this document) and remote procedure invocation (called *RPC* in the rest of this document) for interfacing modules together.

Writing conventions
===================

When variables are described, the convention ``variable: Type`` is used, with ``variable`` having its first letter in lower case and ``Type`` in upper case, and using *CamelCase* for multiple words.
Optional default values are given after the name: ``variable = value: Type``, if ``value`` is ``none``, the argument is optional.

When RPC or messages are described, the convention ``name(arguments) -> ReturnType`` is used, in CamelCase as well.

Concepts
========

Distribution System
  This API aims at abstracting the database back-end in a way that would allow the latter to scale to a distributed system, conceptually able to scale to encompass all running robots on earth. This implies that the API will talk to a local node that cannot hold the whole data, and that data can be lost if part of the network collapses. This is shocking from a database point of view, but perfectly reasonable from a robotics perspective.

Frame
  A frame is a coordinate frame located in 3-D space, linked to other frames through a set of uncertain transformations at different times.
  The uncertainty of the transformations are represented by a probability distribution over SE(3).
  Frames have unique identifiers, and can optionally have human-readable names.
  Frames can have user-specified, arbitrary data attached.

Link
  A probabilistic transformation between two frames, with a label and a time stamp.
  Several links are allowed between the same two frames.
  Links can have user-specified, arbitrary data attached.

Estimated Frame Set
  A graph of frames can be relaxed to estimate deterministic poses, with respect to a given frame.
  The result is a set of transforms in SE(3), each associated to a frame, and a common origin and time.
  The back-end is free to define its own strategy, and different strategies can lead to different relaxed values.
  It is the responsibility of the back-end to document its strategy.
  
Data
  Arbitrary binary user data can be attached to Frames and Links.

Transaction
  All map queries (excepted trigger bookkeeping) must be performed in a transaction, during which the world is assured to be consistent when viewed from the client.
  A transaction might fail in case of write conflict.
  This approach ensures the `Atomicity` and `Consistency` properties of the commonly used ACID model in database literature, but does not ensure `Isolation` (parallel writes working on different parts of a distributed database might break this property) or `Durability` (a sufficiently-large number of servers dying in a distributed database might result in data loss, as total replicability is not a realistic goal). We do not even aim at an *eventual consistency* model because the typical amount of data produced by modern robotic systems hinders the notion of complete replicas.
  The suggested paradigm for implementing transactions is *multiversion concurrency control*.

Trigger
  Clients can create triggers to watch part of the database and be notified of changes asynchronously.

Data types used in interfaces
=============================

Data types are specified in detail to ensure cross-implementation compatibility. They are in general chosen to be generous enough in what they can represent, in order to avoid overflows and limitations. Client libraries are allowed to use other and shorter data types (for instance ``ROS::Time`` for ``TimeStamp`` in ROS) for the ease of interfacing, but the back-end must use the proposed types.

For a given type ``T``, we implicitly defines ``Ts`` to be a list of ``T``. We assume common scalar types (bool, int, float) to be available and defined implicitly.

``TransactionId``
  The unique identifier of a transaction, a ``Uint64``.
``TimeStamp``
  A high-precision time stamp, a tuple ``(seconds: Int64, nanoseconds: Int32)`` in which the first element represents the number of seconds since 1 January 1970, not counting leap seconds (POSIX time), and the second element in the nano-second sub-precision. 
``Interval``
  A tuple ``(start:TimeStamp, end:TimeStamp)`` denoting a time interval.
``FrameId``
  The unique identifier of a frame, a ``Uint64``.
``FrameName``
  A human-readable text naming important frames, like "world", a ``String``.
  Frame names are unique in the whole system.
``Transform``
  A deterministic 3-D transformation, in SE(3), implemented as a 4x4 matrix of ``Float64``.
``AttachedTransform``
  A transformation attached to a frame (the coordinate frame this transform defines), a tuple ``(id: FrameId, transform: Transform)``.
``EstimatedFrameSet``
  The result of a graph relaxation operation.
  A structure ``(origin: FrameId, time: Interval, estimates: AttachedTransform)``, in which ``estimates`` are frames expressed with respect to ``origin``, during the interval ``time``.
``UncertainTransform``
  An uncertain 3-D transformation in SE(3), composed of a ``Transform`` and a Gaussian uncertainty of the transformation in the tangent space of SE(3) (a 6x6 covarience matrix).
``Label``
  A string attached to a link informing on its type, and that can be used to select the link.
``LinkId``
  The unique identifier of a link, a ``Uint64``.
``Link``
  A structure ``(childFrame: FrameId, parentFrame: FrameId, label: Label, time: TimeStamp, transformation: UncertainTransform, confidence: Float64)``.
  This structure links ``childFrame`` to ``parentFrame``, by expressing how to transform points from the first to the second, with uncertainty and at a given ``time``.
  The ``confidence`` value expresses how much the link creator was confident that this link actually exists. This is not the same information as ``transformation``, which expresses an uncertain transformation of points from ``childFrame`` to ``parentFrame``, assuming that the link exists.
``DataType``
  A type of data to be attached to a frame or a link, a ``String``.
``DataBlob``
  Opaque binary data.
``Data``
  Data with type as a tuple ``(type: DataType, value: DataBlob)``
``FrameDataSet``
  A (multi)map of ``FrameId -> Data``.
``LinkDataSet``
  A (multi)map of ``LinkIds -> Data``.
``Box``
  A three-dimensional box in space defined by its two opposite corners, hence a pair of tuples ``((xmin: Float64, ymin: Float64, zmin: Float64), (xmax: Float64, ymax: Float64, zmax: Float64))``.
``TriggerId``: any of { ``TriggerLinkChangedId``, ``TriggerPoseChangedId``, ``TriggerFrameDataChangedId``, ``TriggerLinkDataChangedId`` }
  Trigger identifiers; because these refer to the transport mechanism and not to the database scheme, their types are implementation-dependent.

Some data types are filters select links:
    
``TimeFilter``
  A strategy to filter by time.
  A tuple ``(time: Interval, strategy: String)`` defining an interval and a strategy to interpret it, specific to the back-end.
  All back-ends should implement the following values for ``strategy``: "earliest", "interval", "latest", "closest" that respectively select the earliest link, all links, the most recent link, and the closest link to start time (even outside interval) that match other criteria during ``time``.
``LabelFilter``
  A strategy to filter by label.
  A tuple ``(labels: Labels, strategy: String)`` defining a list of labels and a strategy to interpret it, specific to the back-end.
  All back-ends should implement the following values for ``strategy``: "in", "out" that select all links whose labels are contained in, respectively excluded from, ``labels``.
  
Map queries (RPC)
=================

We assume that the RPC mechanism provides a way to report failures in calls, either through exceptions or an additional return value.
If any call fail within a transaction, the transaction is considered a failure and all subsequent calls will fail, including the commit of the transaction.

Transaction
-----------

``startTransaction() -> TransactionId``
  Create a new transaction and return its identifier.
``commitTransaction(transaction: TransactionId) -> (Bool, String)``
  Attempt to commit a transaction, return whether it succeeded or failed, and the message.
  Read-only transactions always succeed.
  Transactions involving write might fail if there is a write conflict.
  The granularity of their detection depends on the implementation.
``abortTransaction(transaction: TransactionId, reason: String)``
  Abort a transaction, giving a reason for server logs.
  
**All further messages in this section are assumed to take a ``TransactionId`` as first parameter.
For clarity, these are not written explicitly in the following RPC signatures.**
If an object-oriented approach is taken for implementation, these messages can be methods of a ``Transaction`` object.

Spacial selection and relaxation
--------------------------------

``estimateFrames(origin: FrameId, links: LinkIds) -> EstimatedFrameSet``
  Estimate deterministic pose of all frames in ``links``, relative to ``origin``.
  The frame ``origin`` must be included in ``links``, which must all be directly or indirectly connected.
  The returned frames' coordinates are relative to ``origin``.
``estimateFramesWithinBox(origin: FrameId, box: Box, timeFilter = none: TimeFilter, labelFilter = none: LabelFilter) -> EstimatedFrameSet``
  Estimate deterministic pose of all frames linked to ``origin`` within ``box`` (relative to ``origin``), optionally filtered by time and label.
  The returned frames' coordinates are relative to ``origin``.
  The back-end is free to select its strategy to interpret `within` with respect to the uncertainty of the transformations, and to select its own relaxation strategy.
``estimateFramesWithinSphere(origin: FrameId, radius: Float64, timeFilter = none: TimeFilter, labelFilter = none: LabelFilter) -> EstimatedFrameSet``
  Estimate deterministic pose of all frames linked to ``origin`` within ``radius`` (centred on ``origin``), optionally filtered by time and label.
  The returned frames' coordinates are relative to ``origin``.
  The back-end is free to select its strategy to interpret `within` with respect to the uncertainty of the transformations, and to select its own relaxation strategy.
``estimateNeighbourFrames(origin: FrameId, neighbourDist: Uint64, radius: Float64, timeFilter = none: TimeFilter, labelFilter = none: LabelFilter) -> EstimatedFrameSet``
  Estimate deterministic pose of frames linked to ``origin``, within ``radius`` (centred on ``origin``) and at maximum ``neighbourDist`` number of frames away in the graph, optionally filtered by time and label.
  The returned frames' coordinates are relative to ``origin``.
  The back-end is free to select its strategy to interpret `within` with respect to the uncertainty of the transformations, and to select its own relaxation strategy.
``getLinks(frames: FrameIds, neighbourDist = 0: Uint64, timeFilter = none: TimeFilter, labelFilter = none: LabelFilter) -> LinkIds``
  Return all links between any of two ``frames`` and neighbour frames up to a maximum of ``neighbourDist`` number of frames away in the graph, filtered by time and label.

    
Data access
-----------
  
``resolveLinks(links: LinkIds) -> Links``
  Return requested links, if they exist.
``getFrameData(frames: FrameIds, types: DataTypes) -> FrameDataSet``
  Return all data of ``types`` contained in ``frames``.
``getLinkData(links: LinkIds, types: DataTypes) -> LinkDataSet``
  Return all data of ``types`` contained in ``links``.
``getFrameName(frame: FrameId) -> String``
  Get the human-readable name of a frame.
  Because this call require accessing a global name registry, it might take time to complete.
``getFrameId(name: String) -> FrameId``
  Return the identifier of a frame of a given ``name``.
  Because this call require accessing a global name registry, it might take time to complete.

Setters
-------

``setLink(Link: content, reuseId = none: LinkId) -> LinkId``
  Set a link between two frames and return its identifier.
  If ``reuseId`` is given, reuse this identifier instead of creating a new one, and keep attached data.
``deleteLinks(links: LinkIds)``
  Remove links between two frames.
``setFrameData(frame: FrameId, Data: data)``
  Set data for ``frame``, if ``data.type`` already exists, the corresponding data are overwritten.
``deleteFrameData(frame: FrameId, type: DataType)``
  Delete data of a give type in a given frame.
``setLinkData(link: LinkId, Data: data)``
  Set data for ``link``, if ``data.type`` already exists, the corresponding data are overwritten.
``deleteLinkData(link: LinkId, type: DataType)``
  Delete data of a give type in a given link.
``createFrame(name = none: String) -> FrameId``
  Create and return a new FrameId, which is guaranteed to be unique.
  Optionally pass a name.
  If a name is passed, this call requires accessing a global name registry, and therefore might take time to complete.
``setFrameName(frame: FrameId, name: String)``
  Set the human-readable name of a frame.
  Fails if frame does not exist.
  Because this call require accessing a global name registry, it might take time to complete.
``deleteFrame(frame: FrameId)``
  Delete a frame, all its links and all its data.
  Because this call might require accessing a global name registry, it might take time to complete.

  
Triggers (messages)
===================

Available types
---------------

``linksChanged(added: LinkIds, removed: LinkIds, modified: LinkIds)`` referred by ``TriggerLinkChangedId``
  Links have been added to or removed from a set of watched frames.
``estimatedFramesMoved(frames: FrameIds, origin: FrameId)`` referred by ``TriggerPoseChangedId``
  The estimated pose of a set of frames have been moved with respect to ``origin``.
``frameDataChanged(frames: FrameIds, type: DataType)`` referred by ``TriggerFrameDataChangedId``
  Data have been changed for a set of watched frames and a data type.
``linkDataChanged(links: LinkIds, type: DataType)`` referred by ``TriggerLinkDataChangedId``
  Data have been changed for a set of watched links and a data type.

    SM: FIXME: should we have a trigger for frame removed as well? It would be nice for consistency, but practically this seems a rare use case.
  
Trigger book-keeping
--------------------

These trigger bookkeeping queries do not operate within transactions and might fail, by returning invalid trigger identifiers.

``watchLinks(frames: FrameIds, labelFilter = none: LabelFilter, existingTrigger = none: TriggerLinkChangedId) -> TriggerLinkChangedId``
  Watch a set of frames for changes of their links (addition, removal, value modification), optionally filtered by labels, and return the trigger identifier.
  Optionally reuse an existing trigger of the same type.
  All frames must exist, otherwise this query fails.
``watchEstimatedTransforms(frames: FrameIds, origin: FrameId, epsilon: (Float64, Float64), labelFilter = none: LabelFilter, existingTrigger = none: TriggerPoseChangedId) -> TriggerPoseChangedId``
  Watch a set of frames for estimated pose changes with respect to ``origin``, optionally filtered by labels, and return the trigger identifier.
  Set the threshold in (translation, rotation) below which no notification occurs.
  All frames must exist and have a link to origin, otherwise this query fails.
``watchFrameData(frames: FrameIds, type: DataType, existingTrigger = none: TriggerFrameDataChangedId) -> TriggerFrameDataChangedId``
  Watch a set of frames for data changes, return the trigger identifier.
  Optionally reuse an existing trigger of the same type.
  All frames must exist, otherwise this query fails.
``watchLinkData(links: LinkIds, type: DataType, existingTrigger = none: TriggerLinkDataChangedId) -> TriggerLinkDataChangedId``
  Watch a set of links for data changes, return the trigger identifier.
  Optionally reuse an existing trigger of the same type.
  All frames must exist, otherwise this query fails.
``deleteTriggers(triggers: TriggerIds)``
  Delete triggers if they exist.


Notes for distributed implementations
=====================================
 
Unique identifiers
------------------
 
In this documents, unique identifiers (``FrameId`` and ``LinkId``) have type ``Uint64``, whose range is large enough to refer objects between the client and the back-end.
However, in a distributed system where multiple back-ends have to communicate asynchronously, this might not be large enough.
In such a system, we propose to use a 32-byte identifier.
The first 16 bytes shall identify the host (for instance holding an IPv6 address); in a centralised system, these can be 0.
The last 16 bytes shall implement an identifier that is unique on this host, for instance an ever-increasing number.
The identifier space generated by 16 bytes is large enough such the host will never produce the same number twice during its life time.
The back-end shall provide a bijective mapping between the identifiers used by the API and the ones used between back-ends.
