from .rocksdict import *

__doc__ = rocksdict.__doc__

__all__ = ["Rdict",
           "WriteBatch",
           "SstFileWriter",
           "AccessType",
           "WriteOptions",
           "Snapshot",
           "RdictIter",
           "Options",
           "ReadOptions",
           "ColumnFamily",
           "IngestExternalFileOptions",
           "DBPath",
           "MemtableFactory",
           "BlockBasedOptions",
           "PlainTableFactoryOptions",
           "CuckooTableOptions",
           "UniversalCompactOptions",
           "UniversalCompactionStopStyle",
           "SliceTransform",
           "DataBlockIndexType",
           "BlockBasedIndexType",
           "Cache",
           "ChecksumType",
           "DBCompactionStyle",
           "DBCompressionType",
           "DBRecoveryMode",
           "Env",
           "FifoCompactOptions",
           "CompactOptions",
           "BottommostLevelCompaction",
           "KeyEncodingType",
           "DbClosedError",
           "WriteBufferManager",
           "Checkpoint"]

Rdict.__enter__ = lambda self: self
Rdict.__exit__ = lambda self, exc_type, exc_val, exc_tb: self.close()
