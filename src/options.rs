use libc::size_t;
use pyo3::prelude::*;
use rocksdb::{DBPath, Options};
use std::os::raw::{c_int, c_uint};

#[pyclass(name = "Options")]
pub(crate) struct OptionsPy(pub(crate) Options);

// TODO: Path issue
// TODO: table factories options
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
    //
    // pub fn set_prefix_extractor(&mut self, prefix_extractor: SliceTransform) {
    //     self.0.set_prefix_extractor(prefix_extractor)
    // }
    //
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

    // pub fn set_skip_log_error_on_recovery(&mut self, enabled: bool) {
    //     self.0.set_skip_log_error_on_recovery(enabled)
    // }
    //
    // pub fn set_allow_os_buffer(&mut self, is_allow: bool) {
    //     self.0.set_allow_os_buffer(is_allow)
    // }

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

    // pub fn set_max_background_compactions(&mut self, n: c_int) {
    //     self.0.set_max_background_compactions(n)
    // }
    //
    // pub fn set_max_background_flushes(&mut self, n: c_int) {
    //     self.0.set_max_background_flushes(n)
    // }

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

    // pub fn set_memtable_factory(&mut self, factory: MemtableFactory) {
    //     self.0.set_memtable_factory(factory)
    // }
    //
    // pub fn set_block_based_table_factory(&mut self, factory: &BlockBasedOptions) {
    //     self.0.set_block_based_table_factory(factory)
    // }
    //
    // pub fn set_cuckoo_table_factory(&mut self, factory: &CuckooTableOptions) {
    //     self.0.set_cuckoo_table_factory(factory)
    // }
    //
    // pub fn set_plain_table_factory(&mut self, options: &PlainTableFactoryOptions) {
    //     self.0.set_plain_table_factory(options)
    // }

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

    // pub fn set_row_cache(&mut self, cache: &Cache) {
    //     self.0.set_row_cache(cache)
    // }

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
