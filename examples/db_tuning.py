import shutil

from rocksdict import Rdict, Options, SliceTransform, PlainTableFactoryOptions
import os

def db_options():
    opt = Options()
    # create table
    opt.create_if_missing(True)
    # config to more jobs
    opt.set_max_background_jobs(os.cpu_count())
    # configure mem-table to a large value (256 MB)
    opt.set_write_buffer_size(0x10000000)
    opt.set_level_zero_file_num_compaction_trigger(4)
    # configure l0 and l1 size, let them have the same size (1 GB)
    opt.set_max_bytes_for_level_base(0x40000000)
    # 256 MB file size
    opt.set_target_file_size_base(0x10000000)
    # use a smaller compaction multiplier
    opt.set_max_bytes_for_level_multiplier(4.0)
    # use 8-byte prefix (2 ^ 64 is far enough for transaction counts)
    opt.set_prefix_extractor(SliceTransform.create_max_len_prefix(8))
    # set to plain-table for better performance
    opt.set_plain_table_factory(PlainTableFactoryOptions())
    return opt


db = Rdict("./some_path", db_options())
db[0] = 1
db.close()


# automatic reloading all options on reopening
db = Rdict("./some_path")
assert db[0] == 1
db.close()
Rdict.destroy("./some_path", db_options())
shutil.rmtree("./some_path")
