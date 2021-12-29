use libc::size_t;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use rocksdb::{
    BlockBasedIndexType, BlockBasedOptions, Cache, CuckooTableOptions, DataBlockIndexType,
    MemtableFactory, Options, PlainTableFactoryOptions, SliceTransform,
};
use std::os::raw::{c_int, c_uint};

#[pyclass(name = "Options")]
pub(crate) struct OptionsPy(pub(crate) Options);

/// Defines the underlying memtable implementation.
/// See official [wiki](https://github.com/facebook/rocksdb/wiki/MemTable) for more information.
#[pyclass(name = "MemtableFactory")]
pub(crate) struct MemtableFactoryPy(pub(crate) MemtableFactory);

#[pyclass(name = "BlockBasedOptions")]
pub(crate) struct BlockBasedOptionsPy(pub(crate) BlockBasedOptions);

#[pyclass(name = "CuckooTableOptions")]
pub(crate) struct CuckooTableOptionsPy(pub(crate) CuckooTableOptions);

#[pyclass(name = "PlainTableFactoryOptions")]
pub(crate) struct PlainTableFactoryOptionsPy {
    #[pyo3(get, set)]
    user_key_length: u32,

    #[pyo3(get, set)]
    bloom_bits_per_key: i32,

    #[pyo3(get, set)]
    hash_table_ratio: f64,

    #[pyo3(get, set)]
    index_sparseness: usize,
}

#[pyclass(name = "Cache")]
pub(crate) struct CachePy(pub(crate) Cache);

#[pyclass(name = "BlockBasedIndexType")]
pub(crate) struct BlockBasedIndexTypePy(BlockBasedIndexType);

#[pyclass(name = "BlockBasedIndexType")]
pub(crate) struct DataBlockIndexTypePy(DataBlockIndexType);

#[pyclass(name = "SliceTransform")]
pub(crate) struct SliceTransformPy(SliceTransformType);

pub(crate) enum SliceTransformType {
    Fixed(size_t),
    MaxLen(usize),
    NOOP,
}

// TODO: Path issue
// TODO: prefix extractor settings

#[pymethods]
impl OptionsPy {
    #[new]
    pub fn new() -> Self {
        OptionsPy(Options::default())
    }

    pub fn increase_parallelism(&mut self, parallelism: i32) {
        self.0.increase_parallelism(parallelism)
    }

    pub fn optimize_level_style_compaction(&mut self, memtable_memory_budget: usize) {
        self.0
            .optimize_level_style_compaction(memtable_memory_budget)
    }

    pub fn optimize_universal_style_compaction(&mut self, memtable_memory_budget: usize) {
        self.0
            .optimize_universal_style_compaction(memtable_memory_budget)
    }

    pub fn create_if_missing(&mut self, create_if_missing: bool) {
        self.0.create_if_missing(create_if_missing)
    }

    pub fn create_missing_column_families(&mut self, create_missing_cfs: bool) {
        self.0.create_missing_column_families(create_missing_cfs)
    }

    pub fn set_error_if_exists(&mut self, enabled: bool) {
        self.0.set_error_if_exists(enabled)
    }

    pub fn set_paranoid_checks(&mut self, enabled: bool) {
        self.0.set_paranoid_checks(enabled)
    }

    // pub fn set_db_paths(&mut self, paths: &[DBPath]) {
    //     self.0.set_db_paths(paths)
    // }

    // pub fn set_env(&mut self, env: &Env) {
    //     self.0.set_env(env)
    // }

    // pub fn set_compression_type(&mut self, t: DBCompressionType) {
    //     self.0.set_compression_type(t)
    // }

    // pub fn set_compression_per_level(&mut self, level_types: &[DBCompressionType]) {
    //     self.0.set_compression_per_level(level_types])
    // }

    pub fn set_compression_options(
        &mut self,
        w_bits: c_int,
        level: c_int,
        strategy: c_int,
        max_dict_bytes: c_int,
    ) {
        self.0
            .set_compression_options(w_bits, level, strategy, max_dict_bytes)
    }

    pub fn set_zstd_max_train_bytes(&mut self, value: c_int) {
        self.0.set_zstd_max_train_bytes(value)
    }

    pub fn set_compaction_readahead_size(&mut self, compaction_readahead_size: usize) {
        self.0
            .set_compaction_readahead_size(compaction_readahead_size)
    }

    pub fn set_level_compaction_dynamic_level_bytes(&mut self, v: bool) {
        self.0.set_level_compaction_dynamic_level_bytes(v)
    }

    // pub fn set_merge_operator_associative<F: MergeFn + Clone>(&mut self, name: &str, full_merge_fn: F) {
    //     self.0.set_merge_operator_associative(name, full_merge_fn)
    // }
    //
    // pub fn set_merge_operator<F: MergeFn, PF: MergeFn>(&mut self, name: &str, full_merge_fn: F, partial_merge_fn: PF,) {
    //     self.0.set_merge_operator(name, full_merge_fn, partial_merge_fn,)
    // }
    //
    // pub fn add_merge_operator<F: MergeFn + Clone>(&mut self, name: &str, merge_fn: F) {
    //     self.0.add_merge_operator(name, merge_fn)
    // }

    // pub fn set_compaction_filter<F>(&mut self, name: &str, filter_fn: F) {
    //     self.0.set_compaction_filter(name, filter_fn)
    // }
    //
    // pub fn set_compaction_filter_factory<F>(&mut self, factory: F) {
    //     self.0.set_compaction_filter_factory(factory)
    // }
    //
    // pub fn set_comparator(&mut self, name: &str, compare_fn: CompareFn) {
    //     self.0.set_comparator(name, compare_fn)
    // }

    pub fn set_prefix_extractor(
        &mut self,
        prefix_extractor: PyRef<SliceTransformPy>,
    ) -> PyResult<()> {
        let transform = match prefix_extractor.0 {
            SliceTransformType::Fixed(len) => SliceTransform::create_fixed_prefix(len),
            SliceTransformType::MaxLen(len) => match create_max_len_transform(len) {
                Ok(f) => f,
                Err(_) => {
                    return Err(PyException::new_err(
                        "max len prefix only supports len from 1 to 128",
                    ))
                }
            },
            SliceTransformType::NOOP => SliceTransform::create_noop(),
        };
        Ok(self.0.set_prefix_extractor(transform))
    }

    // pub fn add_comparator(&mut self, name: &str, compare_fn: CompareFn) {
    //     self.0.add_comparator(name, compare_fn)
    // }

    pub fn optimize_for_point_lookup(&mut self, cache_size: u64) {
        self.0.optimize_for_point_lookup(cache_size)
    }

    pub fn set_optimize_filters_for_hits(&mut self, optimize_for_hits: bool) {
        self.0.set_optimize_filters_for_hits(optimize_for_hits)
    }

    pub fn set_delete_obsolete_files_period_micros(&mut self, micros: u64) {
        self.0.set_delete_obsolete_files_period_micros(micros)
    }

    pub fn prepare_for_bulk_load(&mut self) {
        self.0.prepare_for_bulk_load()
    }

    pub fn set_max_open_files(&mut self, nfiles: c_int) {
        self.0.set_max_open_files(nfiles)
    }

    pub fn set_max_file_opening_threads(&mut self, nthreads: c_int) {
        self.0.set_max_file_opening_threads(nthreads)
    }

    pub fn set_use_fsync(&mut self, useit: bool) {
        self.0.set_use_fsync(useit)
    }

    // pub fn set_db_log_dir<P: AsRef<Path>>(&mut self, path: P) {
    //     self.0.set_db_log_dir(path)
    // }

    pub fn set_bytes_per_sync(&mut self, nbytes: u64) {
        self.0.set_bytes_per_sync(nbytes)
    }

    pub fn set_wal_bytes_per_sync(&mut self, nbytes: u64) {
        self.0.set_wal_bytes_per_sync(nbytes)
    }

    pub fn set_writable_file_max_buffer_size(&mut self, nbytes: u64) {
        self.0.set_writable_file_max_buffer_size(nbytes)
    }

    pub fn set_allow_concurrent_memtable_write(&mut self, allow: bool) {
        self.0.set_allow_concurrent_memtable_write(allow)
    }

    pub fn set_enable_write_thread_adaptive_yield(&mut self, enabled: bool) {
        self.0.set_enable_write_thread_adaptive_yield(enabled)
    }

    pub fn set_max_sequential_skip_in_iterations(&mut self, num: u64) {
        self.0.set_max_sequential_skip_in_iterations(num)
    }

    pub fn set_use_direct_reads(&mut self, enabled: bool) {
        self.0.set_use_direct_reads(enabled)
    }

    pub fn set_use_direct_io_for_flush_and_compaction(&mut self, enabled: bool) {
        self.0.set_use_direct_io_for_flush_and_compaction(enabled)
    }

    pub fn set_is_fd_close_on_exec(&mut self, enabled: bool) {
        self.0.set_is_fd_close_on_exec(enabled)
    }

    pub fn set_table_cache_num_shard_bits(&mut self, nbits: c_int) {
        self.0.set_table_cache_num_shard_bits(nbits)
    }

    pub fn set_target_file_size_multiplier(&mut self, multiplier: i32) {
        self.0.set_target_file_size_multiplier(multiplier)
    }

    pub fn set_min_write_buffer_number(&mut self, nbuf: c_int) {
        self.0.set_min_write_buffer_number(nbuf)
    }

    pub fn set_max_write_buffer_number(&mut self, nbuf: c_int) {
        self.0.set_max_write_buffer_number(nbuf)
    }

    pub fn set_write_buffer_size(&mut self, size: usize) {
        self.0.set_write_buffer_size(size)
    }

    pub fn set_db_write_buffer_size(&mut self, size: usize) {
        self.0.set_db_write_buffer_size(size)
    }

    pub fn set_max_bytes_for_level_base(&mut self, size: u64) {
        self.0.set_max_bytes_for_level_base(size)
    }

    pub fn set_max_bytes_for_level_multiplier(&mut self, mul: f64) {
        self.0.set_max_bytes_for_level_multiplier(mul)
    }

    pub fn set_max_manifest_file_size(&mut self, size: usize) {
        self.0.set_max_manifest_file_size(size)
    }

    pub fn set_target_file_size_base(&mut self, size: u64) {
        self.0.set_target_file_size_base(size)
    }

    pub fn set_min_write_buffer_number_to_merge(&mut self, to_merge: c_int) {
        self.0.set_min_write_buffer_number_to_merge(to_merge)
    }

    pub fn set_level_zero_file_num_compaction_trigger(&mut self, n: c_int) {
        self.0.set_level_zero_file_num_compaction_trigger(n)
    }

    pub fn set_level_zero_slowdown_writes_trigger(&mut self, n: c_int) {
        self.0.set_level_zero_slowdown_writes_trigger(n)
    }

    pub fn set_level_zero_stop_writes_trigger(&mut self, n: c_int) {
        self.0.set_level_zero_stop_writes_trigger(n)
    }

    // pub fn set_compaction_style(&mut self, style: DBCompactionStyle) {
    //     self.0.set_compaction_style(style)
    // }
    //
    // pub fn set_universal_compaction_options(&mut self, uco: &UniversalCompactOptions) {
    //     self.0.set_universal_compaction_options(uco)
    // }
    //
    // pub fn set_fifo_compaction_options(&mut self, fco: &FifoCompactOptions) {
    //     self.0.set_fifo_compaction_options(fco)
    // }

    pub fn set_unordered_write(&mut self, unordered: bool) {
        self.0.set_unordered_write(unordered)
    }

    pub fn set_max_subcompactions(&mut self, num: u32) {
        self.0.set_max_subcompactions(num)
    }

    pub fn set_max_background_jobs(&mut self, jobs: c_int) {
        self.0.set_max_background_jobs(jobs)
    }

    pub fn set_disable_auto_compactions(&mut self, disable: bool) {
        self.0.set_disable_auto_compactions(disable)
    }

    pub fn set_memtable_huge_page_size(&mut self, size: size_t) {
        self.0.set_memtable_huge_page_size(size)
    }

    pub fn set_max_successive_merges(&mut self, num: usize) {
        self.0.set_max_successive_merges(num)
    }

    pub fn set_bloom_locality(&mut self, v: u32) {
        self.0.set_bloom_locality(v)
    }

    pub fn set_inplace_update_support(&mut self, enabled: bool) {
        self.0.set_inplace_update_support(enabled)
    }

    pub fn set_inplace_update_locks(&mut self, num: usize) {
        self.0.set_inplace_update_locks(num)
    }

    pub fn set_max_bytes_for_level_multiplier_additional(&mut self, level_values: Vec<i32>) {
        self.0
            .set_max_bytes_for_level_multiplier_additional(&level_values)
    }

    pub fn set_skip_checking_sst_file_sizes_on_db_open(&mut self, value: bool) {
        self.0.set_skip_checking_sst_file_sizes_on_db_open(value)
    }

    pub fn set_max_write_buffer_size_to_maintain(&mut self, size: i64) {
        self.0.set_max_write_buffer_size_to_maintain(size)
    }

    pub fn set_enable_pipelined_write(&mut self, value: bool) {
        self.0.set_enable_pipelined_write(value)
    }

    pub fn set_memtable_factory(&mut self, factory: PyRef<MemtableFactoryPy>) {
        self.0.set_memtable_factory(match factory.0 {
            MemtableFactory::Vector => MemtableFactory::Vector,
            MemtableFactory::HashSkipList {
                bucket_count,
                height,
                branching_factor,
            } => MemtableFactory::HashSkipList {
                bucket_count,
                height,
                branching_factor,
            },
            MemtableFactory::HashLinkList { bucket_count } => {
                MemtableFactory::HashLinkList { bucket_count }
            }
        })
    }

    pub fn set_block_based_table_factory(&mut self, factory: PyRef<BlockBasedOptionsPy>) {
        self.0.set_block_based_table_factory(&factory.0)
    }

    pub fn set_cuckoo_table_factory(&mut self, factory: PyRef<CuckooTableOptionsPy>) {
        self.0.set_cuckoo_table_factory(&factory.0)
    }

    pub fn set_plain_table_factory(&mut self, options: PyRef<PlainTableFactoryOptionsPy>) {
        self.0.set_plain_table_factory(&options.to_rocks())
    }

    pub fn set_min_level_to_compress(&mut self, lvl: c_int) {
        self.0.set_min_level_to_compress(lvl)
    }

    pub fn set_report_bg_io_stats(&mut self, enable: bool) {
        self.0.set_report_bg_io_stats(enable)
    }

    pub fn set_max_total_wal_size(&mut self, size: u64) {
        self.0.set_max_total_wal_size(size)
    }

    // pub fn set_wal_recovery_mode(&mut self, mode: DBRecoveryMode) {
    //     self.0.set_wal_recovery_mode(mode)
    // }

    pub fn enable_statistics(&mut self) {
        self.0.enable_statistics()
    }

    pub fn get_statistics(&self) -> Option<String> {
        self.0.get_statistics()
    }

    pub fn set_stats_dump_period_sec(&mut self, period: c_uint) {
        self.0.set_stats_dump_period_sec(period)
    }

    pub fn set_stats_persist_period_sec(&mut self, period: c_uint) {
        self.0.set_stats_persist_period_sec(period)
    }

    pub fn set_advise_random_on_open(&mut self, advise: bool) {
        self.0.set_advise_random_on_open(advise)
    }

    // pub fn set_access_hint_on_compaction_start(&mut self, pattern: AccessHint) {
    //     self.0.set_access_hint_on_compaction_start(pattern)
    // }

    pub fn set_use_adaptive_mutex(&mut self, enabled: bool) {
        self.0.set_use_adaptive_mutex(enabled)
    }

    pub fn set_num_levels(&mut self, n: c_int) {
        self.0.set_num_levels(n)
    }

    pub fn set_memtable_prefix_bloom_ratio(&mut self, ratio: f64) {
        self.0.set_memtable_prefix_bloom_ratio(ratio)
    }

    pub fn set_max_compaction_bytes(&mut self, nbytes: u64) {
        self.0.set_max_compaction_bytes(nbytes)
    }

    // pub fn set_wal_dir<P: AsRef<Path>>(&mut self, path: P) {
    //     self.0.set_wal_dir(path)
    // }

    pub fn set_wal_ttl_seconds(&mut self, secs: u64) {
        self.0.set_wal_ttl_seconds(secs)
    }

    pub fn set_wal_size_limit_mb(&mut self, size: u64) {
        self.0.set_wal_size_limit_mb(size)
    }

    pub fn set_manifest_preallocation_size(&mut self, size: usize) {
        self.0.set_manifest_preallocation_size(size)
    }

    pub fn set_purge_redundant_kvs_while_flush(&mut self, enabled: bool) {
        self.0.set_purge_redundant_kvs_while_flush(enabled)
    }

    pub fn set_skip_stats_update_on_db_open(&mut self, skip: bool) {
        self.0.set_skip_stats_update_on_db_open(skip)
    }

    pub fn set_keep_log_file_num(&mut self, nfiles: usize) {
        self.0.set_keep_log_file_num(nfiles)
    }

    pub fn set_allow_mmap_writes(&mut self, is_enabled: bool) {
        self.0.set_allow_mmap_writes(is_enabled)
    }

    pub fn set_allow_mmap_reads(&mut self, is_enabled: bool) {
        self.0.set_allow_mmap_reads(is_enabled)
    }

    pub fn set_atomic_flush(&mut self, atomic_flush: bool) {
        self.0.set_atomic_flush(atomic_flush)
    }

    pub fn set_row_cache(&mut self, cache: PyRef<CachePy>) {
        self.0.set_row_cache(&cache.0)
    }

    pub fn set_ratelimiter(
        &mut self,
        rate_bytes_per_sec: i64,
        refill_period_us: i64,
        fairness: i32,
    ) {
        self.0
            .set_ratelimiter(rate_bytes_per_sec, refill_period_us, fairness)
    }

    pub fn set_max_log_file_size(&mut self, size: usize) {
        self.0.set_max_log_file_size(size)
    }

    pub fn set_log_file_time_to_roll(&mut self, secs: usize) {
        self.0.set_log_file_time_to_roll(secs)
    }

    pub fn set_recycle_log_file_num(&mut self, num: usize) {
        self.0.set_recycle_log_file_num(num)
    }

    pub fn set_soft_rate_limit(&mut self, limit: f64) {
        self.0.set_soft_rate_limit(limit)
    }

    pub fn set_hard_rate_limit(&mut self, limit: f64) {
        self.0.set_hard_rate_limit(limit)
    }

    pub fn set_soft_pending_compaction_bytes_limit(&mut self, limit: usize) {
        self.0.set_soft_pending_compaction_bytes_limit(limit)
    }

    pub fn set_hard_pending_compaction_bytes_limit(&mut self, limit: usize) {
        self.0.set_hard_pending_compaction_bytes_limit(limit)
    }

    pub fn set_rate_limit_delay_max_milliseconds(&mut self, millis: c_uint) {
        self.0.set_rate_limit_delay_max_milliseconds(millis)
    }

    pub fn set_arena_block_size(&mut self, size: usize) {
        self.0.set_arena_block_size(size)
    }

    pub fn set_dump_malloc_stats(&mut self, enabled: bool) {
        self.0.set_dump_malloc_stats(enabled)
    }

    pub fn set_memtable_whole_key_filtering(&mut self, whole_key_filter: bool) {
        self.0.set_memtable_whole_key_filtering(whole_key_filter)
    }
}

#[pymethods]
impl MemtableFactoryPy {
    #[staticmethod]
    pub fn vector() -> Self {
        MemtableFactoryPy(MemtableFactory::Vector)
    }

    #[staticmethod]
    pub fn hash_skip_list(bucket_count: usize, height: i32, branching_factor: i32) -> Self {
        MemtableFactoryPy(MemtableFactory::HashSkipList {
            bucket_count,
            height,
            branching_factor,
        })
    }

    #[staticmethod]
    pub fn hash_link_list(bucket_count: usize) -> Self {
        MemtableFactoryPy(MemtableFactory::HashLinkList { bucket_count })
    }
}

#[pymethods]
impl BlockBasedOptionsPy {
    #[new]
    pub fn default() -> Self {
        BlockBasedOptionsPy(BlockBasedOptions::default())
    }

    pub fn set_block_size(&mut self, size: usize) {
        self.0.set_block_size(size)
    }

    pub fn set_metadata_block_size(&mut self, size: usize) {
        self.0.set_metadata_block_size(size)
    }

    pub fn set_partition_filters(&mut self, size: bool) {
        self.0.set_partition_filters(size)
    }

    pub fn set_block_cache(&mut self, cache: PyRef<CachePy>) {
        self.0.set_block_cache(&cache.0)
    }

    pub fn set_block_cache_compressed(&mut self, cache: PyRef<CachePy>) {
        self.0.set_block_cache_compressed(&cache.0)
    }

    pub fn disable_cache(&mut self) {
        self.0.disable_cache()
    }

    pub fn set_bloom_filter(&mut self, bits_per_key: c_int, block_based: bool) {
        self.0.set_bloom_filter(bits_per_key, block_based)
    }

    pub fn set_cache_index_and_filter_blocks(&mut self, v: bool) {
        self.0.set_cache_index_and_filter_blocks(v)
    }

    pub fn set_index_type(&mut self, index_type: PyRef<BlockBasedIndexTypePy>) {
        self.0.set_index_type(match index_type.0 {
            BlockBasedIndexType::BinarySearch => BlockBasedIndexType::BinarySearch,
            BlockBasedIndexType::HashSearch => BlockBasedIndexType::HashSearch,
            BlockBasedIndexType::TwoLevelIndexSearch => BlockBasedIndexType::TwoLevelIndexSearch,
        })
    }

    pub fn set_pin_l0_filter_and_index_blocks_in_cache(&mut self, v: bool) {
        self.0.set_pin_l0_filter_and_index_blocks_in_cache(v)
    }

    pub fn set_pin_top_level_index_and_filter(&mut self, v: bool) {
        self.0.set_pin_top_level_index_and_filter(v)
    }

    pub fn set_format_version(&mut self, version: i32) {
        self.0.set_format_version(version)
    }

    pub fn set_block_restart_interval(&mut self, interval: i32) {
        self.0.set_block_restart_interval(interval)
    }

    pub fn set_index_block_restart_interval(&mut self, interval: i32) {
        self.0.set_index_block_restart_interval(interval)
    }

    pub fn set_data_block_index_type(&mut self, index_type: PyRef<DataBlockIndexTypePy>) {
        self.0.set_data_block_index_type(match index_type.0 {
            DataBlockIndexType::BinarySearch => DataBlockIndexType::BinarySearch,
            DataBlockIndexType::BinaryAndHash => DataBlockIndexType::BinaryAndHash,
        })
    }

    pub fn set_data_block_hash_ratio(&mut self, ratio: f64) {
        self.0.set_data_block_hash_ratio(ratio)
    }
}

#[pymethods]
impl CuckooTableOptionsPy {
    #[new]
    pub fn default() -> Self {
        CuckooTableOptionsPy(CuckooTableOptions::default())
    }

    /// Determines the utilization of hash tables. Smaller values
    /// result in larger hash tables with fewer collisions.
    /// Default: 0.9
    pub fn set_hash_ratio(&mut self, ratio: f64) {
        self.0.set_hash_ratio(ratio)
    }

    /// A property used by builder to determine the depth to go to
    /// to search for a path to displace elements in case of
    /// collision. See Builder.MakeSpaceForKey method. Higher
    /// values result in more efficient hash tables with fewer
    /// lookups but take more time to build.
    /// Default: 100
    pub fn set_max_search_depth(&mut self, depth: u32) {
        self.0.set_max_search_depth(depth)
    }

    /// In case of collision while inserting, the builder
    /// attempts to insert in the next cuckoo_block_size
    /// locations before skipping over to the next Cuckoo hash
    /// function. This makes lookups more cache friendly in case
    /// of collisions.
    /// Default: 5
    pub fn set_cuckoo_block_size(&mut self, size: u32) {
        self.0.set_cuckoo_block_size(size)
    }

    /// If this option is enabled, user key is treated as uint64_t and its value
    /// is used as hash value directly. This option changes builder's behavior.
    /// Reader ignore this option and behave according to what specified in
    /// table property.
    /// Default: false
    pub fn set_identity_as_first_hash(&mut self, flag: bool) {
        self.0.set_identity_as_first_hash(flag)
    }

    /// If this option is set to true, module is used during hash calculation.
    /// This often yields better space efficiency at the cost of performance.
    /// If this option is set to false, # of entries in table is constrained to
    /// be power of two, and bit and is used to calculate hash, which is faster in general.
    /// Default: true
    pub fn set_use_module_hash(&mut self, flag: bool) {
        self.0.set_use_module_hash(flag)
    }
}

#[pymethods]
impl PlainTableFactoryOptionsPy {
    #[new]
    pub fn default() -> Self {
        PlainTableFactoryOptionsPy {
            user_key_length: 0,
            bloom_bits_per_key: 10,
            hash_table_ratio: 0.75,
            index_sparseness: 16,
        }
    }
}

impl PlainTableFactoryOptionsPy {
    /// Used with DBOptions::set_plain_table_factory.
    /// See official [wiki](https://github.com/facebook/rocksdb/wiki/PlainTable-Format) for more
    /// information.
    ///
    /// Defaults:
    ///  user_key_length: 0 (variable length)
    ///  bloom_bits_per_key: 10
    ///  hash_table_ratio: 0.75
    ///  index_sparseness: 16
    pub(crate) fn to_rocks(&self) -> PlainTableFactoryOptions {
        PlainTableFactoryOptions {
            // One extra byte for python object type
            user_key_length: if self.user_key_length > 0 {
                self.user_key_length + 1
            } else {
                0
            },
            bloom_bits_per_key: self.bloom_bits_per_key,
            hash_table_ratio: self.hash_table_ratio,
            index_sparseness: self.index_sparseness,
        }
    }
}

#[pymethods]
impl CachePy {
    /// Create a lru cache with capacity
    #[new]
    pub fn new_lru_cache(capacity: size_t) -> PyResult<CachePy> {
        match Cache::new_lru_cache(capacity) {
            Ok(cache) => Ok(CachePy(cache)),
            Err(e) => Err(PyException::new_err(e.into_string())),
        }
    }

    /// Returns the Cache memory usage
    pub fn get_usage(&self) -> usize {
        self.0.get_usage()
    }

    /// Returns pinned memory usage
    pub fn get_pinned_usage(&self) -> usize {
        self.0.get_pinned_usage()
    }

    /// Sets cache capacity
    pub fn set_capacity(&mut self, capacity: size_t) {
        self.0.set_capacity(capacity)
    }
}

#[pymethods]
impl BlockBasedIndexTypePy {
    /// A space efficient index block that is optimized for
    /// binary-search-based index.
    #[staticmethod]
    pub fn binary_search() -> Self {
        BlockBasedIndexTypePy(BlockBasedIndexType::BinarySearch)
    }

    /// The hash index, if enabled, will perform a hash lookup if
    /// a prefix extractor has been provided through Options::set_prefix_extractor.
    #[staticmethod]
    pub fn hash_search() -> Self {
        BlockBasedIndexTypePy(BlockBasedIndexType::HashSearch)
    }

    /// A two-level index implementation. Both levels are binary search indexes.
    #[staticmethod]
    pub fn two_level_index_search() -> Self {
        BlockBasedIndexTypePy(BlockBasedIndexType::TwoLevelIndexSearch)
    }
}

#[pymethods]
impl DataBlockIndexTypePy {
    /// Use binary search when performing point lookup for keys in data blocks.
    /// This is the default.
    #[staticmethod]
    pub fn binary_search() -> Self {
        DataBlockIndexTypePy(DataBlockIndexType::BinarySearch)
    }

    /// Appends a compact hash table to the end of the data block for efficient indexing. Backwards
    /// compatible with databases created without this feature. Once turned on, existing data will
    /// be gradually converted to the hash index format.
    #[staticmethod]
    pub fn binary_and_hash() -> Self {
        DataBlockIndexTypePy(DataBlockIndexType::BinaryAndHash)
    }
}

#[pymethods]
impl SliceTransformPy {
    #[staticmethod]
    pub fn create_fixed_prefix(len: size_t) -> Self {
        SliceTransformPy(SliceTransformType::Fixed(len))
    }

    ///
    /// prefix max length at `len`
    ///
    #[staticmethod]
    pub fn create_max_len_prefix(len: usize) -> Self {
        SliceTransformPy(SliceTransformType::MaxLen(len))
    }

    #[staticmethod]
    pub fn create_noop() -> Self {
        SliceTransformPy(SliceTransformType::NOOP)
    }
}

#[macro_export]
macro_rules! implement_max_len_transform {
    ($($len:literal),*) => {
        fn create_max_len_transform(len: usize) -> Result<SliceTransform, ()> {
            match len {
                $($len => Ok(SliceTransform::create(
                    "max_len",
                    |slice| {
                        if slice.len() > $len {
                            &slice[0..$len]
                        } else {
                            slice
                        }
                    },
                    None,
                ))),*,
                _ => {
                    Err(())
                }
            }
        }
    };
}

implement_max_len_transform!(
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
    27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50,
    51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74,
    75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98,
    99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117,
    118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128
);
