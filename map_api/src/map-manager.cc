/*
-*- coding: utf-8 -*-
kate: replace-tabs off; tab-width 4; indent-width 4; tab-indents true; indent-mode normal
vim: ts=4:sw=4:noexpandtab
*/

#include <map-api/map-manager.h>

// http://home.roadrunner.com/~hinnant/bloomington/shared_mutex.html

namespace aslam
{
	namespace mapmanager
	{
		using namespaced std;
		using boost::format;
		
		// convenient typedefs for mutex
		typedef proposed::upgrade_mutex         mutex_t;
		typedef proposed::shared_lock<mutex_t>  shared_lock;
		typedef proposed::upgrade_lock<mutex_t> upgrade_lock;
		typedef      std::unique_lock<mutex_t>  unique_lock;
		
		
		bool Transaction::hasConflict()
		{
			for o in changedObjects
				if o not in object
					return true;
				if o in object has revision later than this->revision
					return true;
			return false;
		}
		
		template<typename T>
		bool Transaction::checkWriteConflict(const VersionedMap<typename T>& container, UOID object)
		{
			bool inDb;
			shared_lock sl(mapManager->dbMutex);
			RevisionNumber latestRevInDb(container.getLatestRevNumber(object), &inDb);
			sl.unlock();
			if (inDb && latestRevInDb > snapshotRev)
			{
				valid = false;
				throw WriteConflict(str(
					format("Write conflict for object %1 (%2): version in db (revision %3) is more recent than the snapshot this transaction views (revision %4).") % object % typeid(T).name() % latestRevInDb % snapshotRev
				));
			}
			return inDb;
		}
		
		// sanity check for invalid transaction
		#define THROW_IF_INVALID \
			if (!valid) throw InvalidTransaction(String("Calling ") + __func__ + " on an invalidated transaction.");
		
		bool Transaction::isValid() const
		{
			return valid;
		}
		
		FrameId Transaction::createFrame(const String& name)
		{
			THROW_IF_INVALID
			
			// Get a new identifier
			unique_lock l(mapManager->dbMutex);
			FrameId frame(mapManager->nextUOID++);
			l.unlock();
			
			// Set name locally
			updatedFrames[frame] = {name};
			return frame;
		}
		
		void Transaction::setFrameName(FrameId frame, const String& name)
		{
			THROW_IF_INVALID
			
			const bool inDb(checkWriteConflict(mapManager->frames));
			
			if (removedFrames.find(frame) != removedFrames.end())
			{
				valid = false;
				throw InvalidParameters("Setting the name of an already-removed frame");
			}
			
			// Check if frame exists locally
			auto frameIt(updatedFrames.find(frame));
			if (frameIt != updatedFrames.end())
			{
				// Yes, change local copy
				frameIt->name = name;
			}
			else
			{
				// No, if frame exists nowhere, it is an invalid parameter
				if (!inDb)
				{
					valid = false;
					throw InvalidParameters(str(
						format("Frame id %1 does not exist locally or in database") % frame
					));
				}
				
				// Create local copy from database
				shared_lock sl(mapManager->dbMutex);
				updatedFrames[frame] = mapManager->getAtRev(frame, snapshotRev);
			}
		}
		
		void Transaction::deleteFrame(FrameId frame)
		{
			THROW_IF_INVALID
			
			const bool inDb(checkWriteConflict(mapManager->frames));
			
			// Check if frame exists locally
			auto frameIt(updatedFrames.find(frame));
			if (frameIt != updatedFrames.end())
			{
				// Yes, remove from local update list
				updatedFrames.erase(frameIt);
				// In db?
				if (inDb)
				{
					// yes, add to delete list
					removedFrames.insert(frame);
				}
				// no, just forget about it
			}
			else
			{
				// No, if frame exists nowhere, it is an invalid parameter
				if (!inDb)
				{
					valid = false;
					throw InvalidParameters(str(
						format("Frame id %1 does not exist locally or in database") % frame
					));
				}
				// add to delete list
				removedFrames.insert(frame);
			}
		}
		
		MapManager::MapManager():
			currentRev(0),
			nextUOID(0)
		{
		
		}
		
		Transaction* MapManager::startTransaction()
		{
			unique_lock lock(dbMutex);
			// acquire exclusive db lock
			Transaction* transaction(new Transaction(this, currentRev));
			currentTransactions.push_back(transaction);
			// get revision
			// snapshot db
			// release db lock
			
			// acquired transaction lock
			// allocate index
			// put into transaction list
			// release transaction lock
			
			//TODO TODO TODO
		}
		
		TransactionCommitResult MapManager::commitTransaction(Transaction* transaction)
		{
			// If transaction was invalid, abort it and return an error
			if (!transaction->isValid())
			{
				abortTransaction(transaction, "Invalid transaction");
				return TransactionCommitResult(false, "Invalid transaction");
			}
			
			// Acquire shared db lock
			shared_lock sl(dbMutex);
			while (true)
			{
				// For all write operations in the transaction, check whether
				// these conflict with an already-written other operation
				bool conflict(false);
				// TODO
				
				// Try to obtain upgrade ownership
				upgrade_lock ul(move(sl), try_to_lock);
				if (ul.owns_lock())
				{
					// This is the one thread that was able to get upgrade ownership;
					// other threads can still share the database
					
					// Prepare data to write
					// nothing for now
					
					// Now wait until all other threads release shared ownership:
					unique_lock el(move(ul));
					// This thread now has exclusive ownership.
					
					// If there was no conflict, apply write operations
					if (!conflict)
					{
						// Apply write operations
						frames.updateObjects(transaction->updatedFrames);
						frames.removeObjects(transaction->removedFrames);
						// Increment revision number
						++currentRev;
					}
					
					// Remove from transaction list and garbage collect old data
					endTransaction(transaction);
					
					// Notify all sleeping threads about the change
					dbWriteCv.notify_all();
					
					// Return 
					return TransactionCommitResult(!conflict, conflict ? "Write conflict" : "Success");
				}
				// Some other thread is trying to write into db, so sleep until 
				// that work is done.
				dbWriteCv.wait(sl);
				// And then recheck for conflict
			}
		}
		
		void MapManager::abortTransaction(Transaction* transaction, const String& reason)
		{
			unique_lock lock(dbMutex);
			endTransaction(transaction);
		}
		
		// must be called within an exclusive lock
		void MapManager::endTransaction(Transaction* transaction)
		{
			// Remove from ongoing list 
			ongoingTransactions.erase(transaction);
			
			// Garbage collect old data
			// retrieve oldest needed
			UOID oldestRequiredRevision(currentRev);
			for (auto it(ongoingTransactions.begin()); it != ongoingTransactions.end(); ++it)
				oldestRequiredRevision = min(oldestRequiredRevision, it->snapshotRev);
			// remove older
			frames.removeOlderThan(oldestRequiredRevision);
			
			// Delete
			delete transaction;
		}
	}
}
