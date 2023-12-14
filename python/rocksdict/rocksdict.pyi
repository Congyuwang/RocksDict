from typing import Any, Union, List, Iterator, Optional, Tuple, Dict, Callable

__all__ = [
    "Rdict",
    "RdictIter",
    "Options",
    "WriteOptions",
    "ReadOptions",
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
    "SstFileWriter",
    "IngestExternalFileOptions",
    "WriteBatch",
    "ColumnFamily",
    "AccessType",
    "Snapshot",
    "CompactOptions",
    "BottommostLevelCompaction",
    "KeyEncodingType",
    "DbClosedError",
]

class DataBlockIndexType:
    @staticmethod
    def binary_and_hash() -> DataBlockIndexType: ...
    @staticmethod
    def binary_search() -> DataBlockIndexType: ...

class BlockBasedIndexType:
    @staticmethod
    def hash_search() -> BlockBasedIndexType: ...
    @staticmethod
    def binary_search() -> BlockBasedIndexType: ...
    @staticmethod
    def two_level_index_search() -> BlockBasedIndexType: ...

class ChecksumType:
    @staticmethod
    def no_checksum() -> ChecksumType: ...
    @staticmethod
    def crc32c() -> ChecksumType: ...
    @staticmethod
    def xxhash() -> ChecksumType: ...
    @staticmethod
    def xxhash64() -> ChecksumType: ...
    @staticmethod
    def xxh3() -> ChecksumType: ...

class BlockBasedOptions:
    def __init__(self) -> None: ...
    def disable_cache(self) -> None: ...
    def set_block_cache(self, cache: Cache) -> None: ...
    def set_block_restart_interval(self, interval: int) -> None: ...
    def set_block_size(self, size: int) -> None: ...
    def set_bloom_filter(self, bits_per_key: int, block_based: bool) -> None: ...
    def set_cache_index_and_filter_blocks(self, v: bool) -> None: ...
    def set_data_block_hash_ratio(self, ratio: float) -> None: ...
    def set_data_block_index_type(self, index_type: DataBlockIndexType) -> None: ...
    def set_format_version(self, version: int) -> None: ...
    def set_index_block_restart_interval(self, interval: int) -> None: ...
    def set_index_type(self, index_type: BlockBasedIndexType) -> None: ...
    def set_metadata_block_size(self, size: int) -> None: ...
    def set_partition_filters(self, size: bool) -> None: ...
    def set_pin_l0_filter_and_index_blocks_in_cache(self, v: bool) -> None: ...
    def set_pin_top_level_index_and_filter(self, v: bool) -> None: ...
    def set_checksum_type(self, checksum_type: ChecksumType) -> None: ...

class Cache:
    def __init__(self, capacity: int) -> None: ...
    @staticmethod
    def new_hyper_clock_cache(capacity: int, estimated_entry_charge: int) -> Cache: ...
    def get_pinned_usage(self) -> int: ...
    def get_usage(self) -> int: ...
    def set_capacity(self, capacity: int) -> None: ...

class CuckooTableOptions:
    def __init__(self) -> None: ...
    def set_cuckoo_block_size(self, size: int) -> None: ...
    def set_hash_ratio(self, ratio: float) -> None: ...
    def set_identity_as_first_hash(self, flag: bool) -> None: ...
    def set_max_search_depth(self, depth: int) -> None: ...
    def set_use_module_hash(self, flag: bool) -> None: ...

class DBCompactionStyle:
    @staticmethod
    def fifo() -> DBCompactionStyle: ...
    @staticmethod
    def level() -> DBCompactionStyle: ...
    @staticmethod
    def universal() -> DBCompactionStyle: ...

class DBCompressionType:
    @staticmethod
    def bz2() -> DBCompressionType: ...
    @staticmethod
    def lz4() -> DBCompressionType: ...
    @staticmethod
    def lz4hc() -> DBCompressionType: ...
    @staticmethod
    def none() -> DBCompressionType: ...
    @staticmethod
    def snappy() -> DBCompressionType: ...
    @staticmethod
    def zlib() -> DBCompressionType: ...
    @staticmethod
    def zstd() -> DBCompressionType: ...

class DBPath:
    def __init__(self, path: str, target_size: int) -> None: ...

class DBRecoveryMode:
    @staticmethod
    def absolute_consistency() -> DBRecoveryMode: ...
    @staticmethod
    def point_in_time() -> DBRecoveryMode: ...
    @staticmethod
    def skip_any_corrupted_record() -> DBRecoveryMode: ...
    @staticmethod
    def tolerate_corrupted_tail_records() -> DBRecoveryMode: ...

class Env:
    def __init__(self) -> None: ...
    def join_all_threads(self) -> None: ...
    def lower_high_priority_thread_pool_cpu_priority(self) -> None: ...
    def lower_high_priority_thread_pool_io_priority(self) -> None: ...
    def lower_thread_pool_cpu_priority(self) -> None: ...
    def lower_thread_pool_io_priority(self) -> None: ...
    @staticmethod
    def mem_env() -> Env: ...
    def set_background_threads(self, num_threads: int) -> None: ...
    def set_bottom_priority_background_threads(self, n: int) -> None: ...
    def set_high_priority_background_threads(self, n: int) -> None: ...
    def set_low_priority_background_threads(self, n: int) -> None: ...

class FifoCompactOptions:
    @property
    def max_table_files_size(self) -> int: ...
    @max_table_files_size.setter
    def max_table_files_size(self, v: int) -> None: ...
    def __init__(self) -> None: ...

class FlushOptions:
    @property
    def wait(self) -> bool: ...
    @wait.setter
    def wait(self, v: bool) -> None: ...
    def __init__(self) -> None: ...

class MemtableFactory:
    @staticmethod
    def hash_link_list() -> MemtableFactory: ...
    @staticmethod
    def hash_skip_list() -> MemtableFactory: ...
    @staticmethod
    def vector() -> MemtableFactory: ...

class Options:
    def __init__(self, raw_mode: bool = False) -> None: ...
    @staticmethod
    def load_latest(
        path: str,
        env: Env = Env(),
        ignore_unknown_options: bool = False,
        cache: Cache = Cache(8 * 1024 * 1024),
    ) -> Tuple[Options, Dict[str, Options]]: ...
    def create_if_missing(self, create_if_missing: bool) -> None: ...
    def create_missing_column_families(self, create_missing_cfs: bool) -> None: ...
    def enable_statistics(self) -> None: ...
    def get_statistics(self) -> Union[str, None]: ...
    def increase_parallelism(self, parallelism: int) -> None: ...
    def optimize_for_point_lookup(self, cache_size: int) -> None: ...
    def optimize_level_style_compaction(self, memtable_memory_budget: int) -> None: ...
    def optimize_universal_style_compaction(
        self, memtable_memory_budget: int
    ) -> None: ...
    def prepare_for_bulk_load(self) -> None: ...
    def set_advise_random_on_open(self, advise: bool) -> None: ...
    def set_allow_concurrent_memtable_write(self, allow: bool) -> None: ...
    def set_allow_mmap_reads(self, is_enabled: bool) -> None: ...
    def set_allow_mmap_writes(self, is_enabled: bool) -> None: ...
    def set_arena_block_size(self, size: int) -> None: ...
    def set_atomic_flush(self, atomic_flush: bool) -> None: ...
    def set_block_based_table_factory(self, factory: BlockBasedOptions) -> None: ...
    def set_bloom_locality(self, v: int) -> None: ...
    def set_bytes_per_sync(self, nbytes: int) -> None: ...
    def set_compaction_readahead_size(self, compaction_readahead_size: int) -> None: ...
    def set_compaction_style(self, style: DBCompactionStyle) -> None: ...
    def set_compression_options(
        self, w_bits: int, level: int, strategy: int, max_dict_bytes: int
    ) -> None: ...
    def set_compression_per_level(self, level_types: list) -> None: ...
    def set_compression_type(self, t: DBCompressionType) -> None: ...
    def set_cuckoo_table_factory(self, factory: CuckooTableOptions) -> None: ...
    def set_db_log_dir(self, path: str) -> None: ...
    def set_db_paths(self, paths: list) -> None: ...
    def set_db_write_buffer_size(self, size: int) -> None: ...
    def set_delete_obsolete_files_period_micros(self, micros: int) -> None: ...
    def set_disable_auto_compactions(self, disable: bool) -> None: ...
    def set_dump_malloc_stats(self, enabled: bool) -> None: ...
    def set_enable_pipelined_write(self, value: bool) -> None: ...
    def set_enable_write_thread_adaptive_yield(self, enabled: bool) -> None: ...
    def set_env(self, env: Env) -> None: ...
    def set_error_if_exists(self, enabled: bool) -> None: ...
    def set_fifo_compaction_options(self, fco: FifoCompactOptions) -> None: ...
    def set_hard_pending_compaction_bytes_limit(self, limit: int) -> None: ...
    def set_inplace_update_locks(self, num: int) -> None: ...
    def set_inplace_update_support(self, enabled: bool) -> None: ...
    def set_is_fd_close_on_exec(self, enabled: bool) -> None: ...
    def set_keep_log_file_num(self, nfiles: int) -> None: ...
    def set_level_compaction_dynamic_level_bytes(self, v: bool) -> None: ...
    def set_level_zero_file_num_compaction_trigger(self, n: int) -> None: ...
    def set_level_zero_slowdown_writes_trigger(self, n_int) -> None: ...
    def set_level_zero_stop_writes_trigger(self, n: int) -> None: ...
    def set_log_file_time_to_roll(self, secs: int) -> None: ...
    def set_manifest_preallocation_size(self, size: int) -> None: ...
    def set_max_background_jobs(self, jobs: int) -> None: ...
    def set_max_bytes_for_level_base(self, size: int) -> None: ...
    def set_max_bytes_for_level_multiplier(self, mul: float) -> None: ...
    def set_max_bytes_for_level_multiplier_additional(
        self, level_values: list
    ) -> None: ...
    def set_max_compaction_bytes(self, nbytes: int) -> None: ...
    def set_max_file_opening_threads(self, nthreads: int) -> None: ...
    def set_max_log_file_size(self, size: int) -> None: ...
    def set_max_manifest_file_size(self, size: int) -> None: ...
    def set_max_open_files(self, nfiles: int) -> None: ...
    def set_max_sequential_skip_in_iterations(self, num: int) -> None: ...
    def set_max_subcompactions(self, num: int) -> None: ...
    def set_max_successive_merges(self, num: int) -> None: ...
    def set_max_total_wal_size(self, size: int) -> None: ...
    def set_max_write_buffer_number(self, nbuf: int) -> None: ...
    def set_max_write_buffer_size_to_maintain(self, size: int) -> None: ...
    def set_memtable_factory(self, factory: MemtableFactory) -> None: ...
    def set_memtable_huge_page_size(self, size: int) -> None: ...
    def set_memtable_prefix_bloom_ratio(self, ratio: float) -> None: ...
    def set_memtable_whole_key_filtering(self, whole_key_filter: bool) -> None: ...
    def set_min_level_to_compress(self, lvl: int) -> None: ...
    def set_min_write_buffer_number(self, nbuf: int) -> None: ...
    def set_min_write_buffer_number_to_merge(self, to_merge: int) -> None: ...
    def set_num_levels(self, n: int) -> None: ...
    def set_optimize_filters_for_hits(self, optimize_for_hits: bool) -> None: ...
    def set_paranoid_checks(self, enabled: bool) -> None: ...
    def set_plain_table_factory(self, options: PlainTableFactoryOptions) -> None: ...
    def set_prefix_extractor(self, prefix_extractor: SliceTransform) -> None: ...
    def set_ratelimiter(
        self, rate_bytes_per_sec: int, refill_period_us: int, fairness: int
    ) -> None: ...
    def set_recycle_log_file_num(self, num: int) -> None: ...
    def set_report_bg_io_stats(self, enable: bool) -> None: ...
    def set_row_cache(self, cache: Cache) -> None: ...
    def set_skip_checking_sst_file_sizes_on_db_open(self, value: bool) -> None: ...
    def set_skip_stats_update_on_db_open(self, skip: bool) -> None: ...
    def set_soft_pending_compaction_bytes_limit(self, limit: int) -> None: ...
    def set_stats_dump_period_sec(self, period: int) -> None: ...
    def set_stats_persist_period_sec(self, period: int) -> None: ...
    def set_table_cache_num_shard_bits(self, nbits: int) -> None: ...
    def set_target_file_size_base(self, size: int) -> None: ...
    def set_target_file_size_multiplier(self, multiplier: int) -> None: ...
    def set_universal_compaction_options(
        self, uco: UniversalCompactOptions
    ) -> None: ...
    def set_unordered_write(self, unordered: bool) -> None: ...
    def set_use_adaptive_mutex(self, enabled: bool) -> None: ...
    def set_use_direct_io_for_flush_and_compaction(self, enabled: bool) -> None: ...
    def set_use_direct_reads(self, enabled: bool) -> None: ...
    def set_use_fsync(self, useit: bool) -> None: ...
    def set_wal_bytes_per_sync(self, nbytes: int) -> None: ...
    def set_wal_dir(self, path: str) -> None: ...
    def set_wal_recovery_mode(self, mode: DBRecoveryMode) -> None: ...
    def set_wal_size_limit_mb(self, size: int) -> None: ...
    def set_wal_ttl_seconds(self, secs: int) -> None: ...
    def set_writable_file_max_buffer_size(self, nbytes: int) -> None: ...
    def set_write_buffer_size(self, size: int) -> None: ...
    def set_zstd_max_train_bytes(self, value: int) -> None: ...

class PlainTableFactoryOptions:
    @property
    def bloom_bits_per_key(self) -> int: ...
    @bloom_bits_per_key.setter
    def bloom_bits_per_key(self, v: int) -> None: ...
    @property
    def hash_table_ratio(self) -> float: ...
    @hash_table_ratio.setter
    def hash_table_ratio(self, v: float) -> None: ...
    @property
    def index_sparseness(self) -> int: ...
    @index_sparseness.setter
    def index_sparseness(self, v: int) -> None: ...
    @property
    def user_key_length(self) -> int: ...
    @user_key_length.setter
    def user_key_length(self, v: int) -> None: ...
    def __init__(self) -> None: ...

class ReadOptions:
    def __init__(self) -> None: ...
    def fill_cache(self) -> None: ...
    def set_background_purge_on_iterator_cleanup(self, v: bool) -> None: ...
    def set_ignore_range_deletions(self, v: bool) -> None: ...
    def set_iterate_lower_bound(
        self, key: Union[str, int, float, bytes, bool]
    ) -> None: ...
    def set_iterate_upper_bound(
        self, key: Union[str, int, float, bytes, bool]
    ) -> None: ...
    def set_max_skippable_internal_keys(self, num: int) -> None: ...
    def set_pin_data(self, v: bool) -> None: ...
    def set_prefix_same_as_start(self, v: bool) -> None: ...
    def set_readahead_size(self, v: int) -> None: ...
    def set_tailing(self, v: bool) -> None: ...
    def set_total_order_seek(self, v: bool) -> None: ...
    def set_verify_checksums(self, v: bool) -> None: ...
    def set_async_io(self, v: bool) -> None: ...

class SliceTransform:
    @staticmethod
    def create_fixed_prefix(len: int) -> SliceTransform: ...
    @staticmethod
    def create_max_len_prefix(len: int) -> SliceTransform: ...
    @staticmethod
    def create_noop() -> SliceTransform: ...

class UniversalCompactOptions:
    @property
    def compression_size_percent(self) -> int: ...
    @compression_size_percent.setter
    def compression_size_percent(self, v: int) -> None: ...
    @property
    def max_merge_width(self) -> int: ...
    @max_merge_width.setter
    def max_merge_width(self, v: int) -> None: ...
    @property
    def max_size_amplification_percent(self) -> int: ...
    @max_size_amplification_percent.setter
    def max_size_amplification_percent(self, v: int) -> None: ...
    @property
    def min_merge_width(self) -> int: ...
    @min_merge_width.setter
    def min_merge_width(self, v: int) -> None: ...
    @property
    def size_ratio(self) -> int: ...
    @size_ratio.setter
    def size_ratio(self, v: int) -> None: ...
    @property
    def stop_style(self) -> UniversalCompactionStopStyle: ...
    @stop_style.setter
    def stop_style(self, style: UniversalCompactionStopStyle) -> None: ...
    def __init__(self) -> None: ...

class UniversalCompactionStopStyle:
    @classmethod
    def __init__(cls, *args, **kwargs) -> None: ...
    def similar(self, *args, **kwargs) -> None: ...
    def total(self, *args, **kwargs) -> None: ...

class WriteOptions:
    @property
    def ignore_missing_column_families(self) -> bool: ...
    @ignore_missing_column_families.setter
    def ignore_missing_column_families(self, v: bool) -> None: ...
    @property
    def low_pri(self) -> bool: ...
    @low_pri.setter
    def low_pri(self, v: bool) -> None: ...
    @property
    def memtable_insert_hint_per_batch(self) -> bool: ...
    @memtable_insert_hint_per_batch.setter
    def memtable_insert_hint_per_batch(self, v: bool) -> None: ...
    @property
    def no_slowdown(self) -> bool: ...
    @no_slowdown.setter
    def no_slowdown(self, v: bool) -> None: ...
    @property
    def sync(self) -> bool: ...
    @sync.setter
    def sync(self, v: bool) -> None: ...
    def __init__(self) -> None: ...
    def disable_wal(self, disable: bool) -> None: ...

class Rdict:
    def __init__(
        self,
        path: str,
        options: Union[Options, None] = None,
        column_families: Union[Dict[str, Options], None] = None,
        access_type: AccessType = AccessType.read_write(),
    ) -> None: ...
    def __enter__(self) -> Rdict: ...
    def set_dumps(self, dumps: Callable[[Any], bytes]) -> None: ...
    def set_loads(self, dumps: Callable[[bytes], Any]) -> None: ...
    def set_read_options(self, read_opt: ReadOptions) -> None: ...
    def set_write_options(self, write_opt: WriteOptions) -> None: ...
    def __contains__(self, key: Union[str, int, float, bytes, bool]) -> bool: ...
    def __delitem__(self, key: Union[str, int, float, bytes, bool]) -> None: ...
    def __getitem__(
        self,
        key: Union[
            str, int, float, bytes, bool, List[Union[str, int, float, bytes, bool]]
        ],
    ) -> Any | None: ...
    def __setitem__(
        self, key: Union[str, int, float, bytes, bool], value: Any
    ) -> None: ...
    def get(
        self,
        key: Union[
            str, int, float, bytes, bool, List[Union[str, int, float, bytes, bool]]
        ],
        default: Any = None,
        read_opt: Union[ReadOptions, None] = None,
    ) -> Any | None: ...
    def put(
        self,
        key: Union[str, int, float, bytes, bool],
        value: Any,
        write_opt: Union[WriteOptions, None] = None,
    ) -> None: ...
    def delete(
        self,
        key: Union[str, int, float, bytes, bool],
        write_opt: Union[WriteOptions, None] = None,
    ) -> None: ...
    def key_may_exist(
        self,
        key: Union[str, int, float, bytes, bool],
        fetch: bool = False,
        read_opt=None,
    ) -> Union[bool, Tuple[bool, Any]]: ...
    def iter(self, read_opt: Union[ReadOptions, None] = None) -> RdictIter: ...
    def items(
        self,
        backwards: bool = False,
        from_key: Union[str, int, float, bytes, bool, None] = None,
        read_opt: Union[ReadOptions, None] = None,
    ) -> RdictItems: ...
    def chunked_items(
        self,
        chunk_size: Optional[int] = None,
        backwards: bool = False,
        from_key: Union[str, int, float, bytes, bool, None] = None,
        read_opt: Union[ReadOptions, None] = None,
    ) -> RdictChunkedItems: ...
    def keys(
        self,
        backwards: bool = False,
        from_key: Union[str, int, float, bytes, bool, None] = None,
        read_opt: Union[ReadOptions, None] = None,
    ) -> RdictKeys: ...
    def chunked_keys(
        self,
        chunk_size: Optional[int] = None,
        backwards: bool = False,
        from_key: Union[str, int, float, bytes, bool, None] = None,
        read_opt: Union[ReadOptions, None] = None,
    ) -> RdictChunkedKeys: ...
    def values(
        self,
        backwards: bool = False,
        from_key: Union[str, int, float, bytes, bool, None] = None,
        read_opt: Union[ReadOptions, None] = None,
    ) -> RdictValues: ...
    def chunked_values(
        self,
        chunk_size: Optional[int] = None,
        backwards: bool = False,
        from_key: Union[str, int, float, bytes, bool, None] = None,
        read_opt: Union[ReadOptions, None] = None,
    ) -> RdictChunkedValues: ...
    def ingest_external_file(
        self,
        paths: List[str],
        opts: IngestExternalFileOptions = IngestExternalFileOptions(),
    ) -> None: ...
    def get_column_family(self, name: str) -> Rdict: ...
    def get_column_family_handle(self, name: str) -> ColumnFamily: ...
    def drop_column_family(self, name: str) -> None: ...
    def create_column_family(
        self, name: str, options: Options = Options()
    ) -> Rdict: ...
    def write(
        self, write_batch: WriteBatch, write_opt: Union[WriteOptions, None] = None
    ) -> None: ...
    def delete_range(
        self,
        begin: Union[str, int, float, bytes, bool],
        end: Union[str, int, float, bytes, bool],
        write_opt: Union[WriteOptions, None] = None,
    ) -> None: ...
    def snapshot(self) -> Snapshot: ...
    def path(self) -> str: ...
    def set_options(self, options: Dict[str, str]) -> None: ...
    def property_value(self, name: str) -> Union[str, None]: ...
    def property_int_value(self, name: str) -> Union[int, None]: ...
    def latest_sequence_number(self) -> int: ...
    def live_files(self) -> List[Dict[str, Any]]: ...
    def compact_range(
        self,
        begin: Union[str, int, float, bytes, bool, None],
        end: Union[str, int, float, bytes, bool, None],
        compact_opt: CompactOptions = CompactOptions(),
    ) -> None: ...
    def try_catch_up_with_primary(self) -> None: ...
    def cancel_all_background(self, wait: bool) -> None: ...
    def close(self) -> None: ...
    def __exit__(self, exc_type, exc_val, exc_tb) -> None: ...
    def flush(self, wait: bool = True) -> None: ...
    def flush_wal(self, sync: bool = True) -> None: ...
    @staticmethod
    def destroy(path: str, options: Options = Options()) -> None: ...
    @staticmethod
    def repair(path: str, options: Options = Options()) -> None: ...
    @staticmethod
    def list_cf(path: str, options: Options = Options()) -> List[str]: ...

class RdictItems(Iterator[Tuple[Union[str, int, float, bytes, bool], Any]]):
    def __iter__(self) -> RdictItems: ...
    def __next__(self) -> Tuple[Union[str, int, float, bytes, bool], Any]: ...

class RdictKeys(Iterator[Union[str, int, float, bytes, bool]]):
    def __iter__(self) -> RdictKeys: ...
    def __next__(self) -> Union[str, int, float, bytes, bool]: ...

class RdictValues(Iterator[Any]):
    def __iter__(self) -> RdictValues: ...
    def __next__(self) -> Any: ...

class RdictChunkedItems(
    Iterator[List[Tuple[Union[str, int, float, bytes, bool]], Any]]
):
    def __iter__(self) -> RdictChunkedItems: ...
    def __next__(self) -> List[Tuple[Union[str, int, float, bytes, bool]], Any]: ...

class RdictChunkedKeys(Iterator[List[Union[str, int, float, bytes, bool]]]):
    def __iter__(self) -> RdictChunkedKeys: ...
    def __next__(self) -> List[Union[str, int, float, bytes, bool]]: ...

class RdictChunkedValues(Iterator[List[Any]]):
    def __iter__(self) -> RdictValues: ...
    def __next__(self) -> List[Any]: ...

class RdictIter:
    def valid(self) -> bool: ...
    def status(self) -> None: ...
    def seek_to_first(self) -> None: ...
    def seek_to_last(self) -> None: ...
    def seek(self, key: Union[str, int, float, bytes, bool]) -> None: ...
    def seek_for_prev(self, key: Union[str, int, float, bytes, bool]) -> None: ...
    def next(self) -> None: ...
    def prev(self) -> None: ...
    def key(self) -> Any: ...
    def value(self) -> Any: ...
    def get_chunk_keys(
        self, chunk_size: Optional[int] = None, backwards: bool = False
    ) -> List[Union[str, int, float, bytes, bool]]: ...
    def get_chunk_values(
        self, chunk_size: Optional[int] = None, backwards: bool = False
    ) -> List[Any]: ...
    def get_chunk_items(
        self, chunk_size: Optional[int] = None, backwards: bool = False
    ) -> List[Tuple[Union[str, int, float, bytes, bool], Any]]: ...

class IngestExternalFileOptions:
    def __init__(self) -> None: ...
    def set_move_files(self, v: bool) -> None: ...
    def set_snapshot_consistency(self, v: bool) -> None: ...
    def set_allow_global_seqno(self, v: bool) -> None: ...
    def set_allow_blocking_flush(self, v: bool) -> None: ...
    def set_ingest_behind(self, v: bool) -> None: ...

class SstFileWriter:
    def __init__(self, options: Options = Options()) -> None: ...
    def set_dumps(self, dumps: Callable[[Any], bytes]) -> None: ...
    def open(self, path: str) -> None: ...
    def finish(self) -> None: ...
    def file_size(self) -> int: ...
    def __setitem__(
        self, key: Union[str, int, float, bytes, bool], value: Any
    ) -> None: ...
    def __delitem__(self, key: Union[str, int, float, bytes, bool]) -> None: ...

class WriteBatch:
    def __init__(self, raw_mode: bool = False) -> None: ...
    def __len__(self) -> int: ...
    def __setitem__(
        self, key: Union[str, int, float, bytes, bool], value: Any
    ) -> None: ...
    def __delitem__(self, key: Union[str, int, float, bytes, bool]) -> None: ...
    def set_dumps(self, dumps: Callable[[Any], bytes]) -> None: ...
    def set_default_column_family(
        self, column_family: Union[ColumnFamily, None]
    ) -> None: ...
    def len(self) -> int: ...
    def size_in_bytes(self) -> int: ...
    def is_empty(self) -> bool: ...
    def put(
        self,
        key: Union[str, int, float, bytes, bool],
        value: Any,
        column_family: Union[ColumnFamily, None] = None,
    ) -> None: ...
    def delete(
        self,
        key: Union[str, int, float, bytes, bool],
        column_family: Union[ColumnFamily, None] = None,
    ) -> None: ...
    def delete_range(
        self,
        begin: Union[str, int, float, bytes, bool],
        end: Union[str, int, float, bytes, bool],
        column_family: Union[ColumnFamily, None] = None,
    ) -> None: ...
    def clear(self) -> None: ...

class ColumnFamily: ...

class AccessType:
    @staticmethod
    def read_write() -> AccessType: ...
    @staticmethod
    def read_only(error_if_log_file_exist: bool = True) -> AccessType: ...
    @staticmethod
    def secondary(secondary_path: str) -> AccessType: ...
    @staticmethod
    def with_ttl(duration: int) -> AccessType: ...

class Snapshot:
    def __getitem__(self, key: Union[str, int, float, bytes, bool]) -> Any: ...
    def iter(self, read_opt: Union[ReadOptions, None] = None) -> RdictIter: ...
    def items(
        self,
        backwards: bool = False,
        from_key: Union[str, int, float, bytes, bool, None] = None,
        read_opt: Union[ReadOptions, None] = None,
    ) -> RdictItems: ...
    def keys(
        self,
        backwards: bool = False,
        from_key: Union[str, int, float, bytes, bool, None] = None,
        read_opt: Union[ReadOptions, None] = None,
    ) -> RdictKeys: ...
    def values(
        self,
        backwards: bool = False,
        from_key: Union[str, int, float, bytes, bool, None] = None,
        read_opt: Union[ReadOptions, None] = None,
    ) -> RdictValues: ...

class BottommostLevelCompaction:
    @staticmethod
    def skip() -> BottommostLevelCompaction: ...
    @staticmethod
    def if_have_compaction_filter() -> BottommostLevelCompaction: ...
    @staticmethod
    def force() -> BottommostLevelCompaction: ...
    @staticmethod
    def force_optimized() -> BottommostLevelCompaction: ...

class CompactOptions:
    def __init__(self) -> None: ...
    def set_exclusive_manual_compaction(self, v: bool) -> None: ...
    def set_bottommost_level_compaction(
        self, lvl: BottommostLevelCompaction
    ) -> None: ...
    def set_change_level(self, v: bool) -> None: ...
    def set_target_level(self, lvl: int) -> None: ...

class KeyEncodingType:
    @staticmethod
    def none() -> KeyEncodingType: ...
    @staticmethod
    def prefix() -> KeyEncodingType: ...

class DbClosedError(Exception):
    """Raised when accessing a closed database instance."""
