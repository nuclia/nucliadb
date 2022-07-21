// All stored data types, must be serialized and deserealized with bincode and will implment
// Default.
//
// The disk structure should be:
// -> stamp.nuclia $empty file that ensures that the current directory is a vectors index
// -> dir.lock  $file use to handle cocurrent accesses to the directory
// -> hnsw.bincode $file with the hnsw index;
// -> transactions/ $directory where the transactions are stored
// -> txn_0/ $transaction with id 0
// -> delete_log.bincode $file with the vectors deleted in this transaction
// -> segment.bincode, $file with the vectors added in this transaction
// -> txn_1/
// -> ...
// -> log.bincode $file that will be used to identify which transactions where processed.
// -> database/  $storage folder for the lmdb
//
//
// this module should provide the following functionality:
// -> given a path, check if is a vectors index, if not initialize
// -> get an unlocked lock to the structure
// -> /* Only if a locked lock is provided */ get the transactions log
// -> /* Only if a locked lock is provided */ get the hnsw
// -> /* Only if a locked lock is provided */ given a txn id, get the delete log
// -> /* Only if a locked lock is provided */ given a txn id, get the mmaped segment
// -> /* Only if a locked lock is provided */ given a txn id and the delete log, write it
// -> /* Only if a locked lock is provided */ given the hnsw, write it
// -> /* Only if a locked lock is provided */ given the transactions log, write it
// -> /* Only if a locked lock is provided */ open and return an instance of the database
// -> /* Only if a locked lock is provided */ create a new txn with a given id and return segment
// and delete_log files ready to be writen to.
//
//
// types:
// Hnsw (the hnsw index), in crate::hnsw::Hnsw;
// TransactionLog (the transaction log ) in crate::index::TransactionLog
// DeleteLog (the delete log) in crate::delete_log::DeleteLog
// Segment (type use for mmaping segments) in crate::segment::Segment
// LMBDStorage (type use for using the lmdb (with open and create)) in crate::database::LMBDStorage;
