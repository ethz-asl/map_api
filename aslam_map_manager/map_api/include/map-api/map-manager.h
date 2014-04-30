/*
-*- coding: utf-8 -*-
kate: replace-tabs off; tab-width 4; indent-width 4; tab-indents true; indent-mode normal
vim: ts=4:sw=4:noexpandtab
*/

#ifndef __ASLAM_MAP_MANAGER_H
#define __ASLAM_MAP_MANAGER_H

#include <stdint.h>

#include <condition_variable>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <utility>
#include <set>
#include <string>
#include <tuple>
#include <vector>

namespace aslam
{
	namespace mapmanager
	{
		// Types for users
		
		typedef uint64_t RevisionNumber;
		typedef uint64_t TransactionId;
		typedef uint64_t UOID;
		typedef UOID FrameId;
		typedef UOID LinkId;
		typedef float UncertainTransform;
		typedef std::string String;
		
		typedef std::vector<uint8_t> Blob;
		typedef std::map<String, std::shared_ptr<Blob> > DataSet;
		
		class MapManager;
		class SymbolIndexer;

		class Frame
		{
			String name;
			DataSet dataSet;
		};
		
		/*class Link
		{
			UncertainTransform transformation;
			FrameId parentFrame;
			FrameId childFrame;
			DataSet dataSet;
		};*/
		
		// Versioning support
		
		typedef std::runtime_error WriteConflict;
		typedef std::runtime_error InvalidParameters;
		typedef std::runtime_error InvalidTransaction;
		
		template<typename T>
		struct VersionedMap
		{
			typedef std::map<RevisionNumber, T> VersionsMap;
			typedef std::map<UOID, VersionsMap> VersionedObjectsMap;
			typedef std::map<UOID, T> ObjectsMap;
			typedef std::set<UOID> IdentifierSet;
			
			typedef std::runtime_error ObjectDoesNotExists;
			
			const T& getAtRev(UOID uoid, RevisionNumber revision) const;
			RevisionNumber getLatestRevNumber(UOID uoid, bool* exists) const;
			void updateObjects(const ObjectsMap& updates);
			void removeObjects(const IdentifierSet& removed);
			
			void removeOlderThan(RevisionNumber revision);
		
		protected:
			VersionedObjectsMap objects;
		};
		
		// Transaction; for read, views a certain revision; for write, queues operations locally
		// data local to the transation only valid from a single thread
		class Transaction
		{
		protected:
			friend class MapManager;
			Transaction(MapManager *mapManager, RevisionNumber revision);
			Transaction(const SymbolIndexer& that) = delete;
			~Transaction();
			
			// check write conflict, return whether the object is in db
			template<typename T>
			bool checkWriteConflict(const VersionedMap<T>& container, UOID object);
			
		public:
			bool isValid() const;
			
			FrameId createFrame(const String& name = "");
			void setFrameName(FrameId frame, const String& name);
			void deleteFrame(FrameId frame);
			
		protected:
			bool valid;
			MapManager *mapManager;
			RevisionNumber snapshotRev;
			
			std::map<UOID, Frame> updatedFrames;
			//std::map<UOID, Link> updatedLinks;
			std::set<UOID> removedFrames;
			//std::set<UOID> deletedLinks;
		};
		
		class MapManager
		{
		public:
			typedef std::tuple<bool, String> TransactionCommitResult;
			
		public:
			// thread-safe
			Transaction* startTransaction();
			
			// thread-safe, destroy parameter
			TransactionCommitResult commitTransaction(Transaction* transaction);
			
			// thread-safe, destroy parameter
			void abortTransaction(Transaction* transaction, const String& reason);
			
		protected:
			// for transaction-based multiple revision
			RevisionNumber currentRev;
			UOID nextUOID;
			std::list<Transaction*> ongoingTransactions;
			
			// data
			VersionedMap<Frame> frames;
			//VersionedMap<Link> links;
			
			// access protection
			mutable std::upgrade_mutex dbMutex;
			mutable std::condition_variable_any dbWriteCv;
		};
	}
}

#endif // __ASLAM_MAP_MANAGER_H
