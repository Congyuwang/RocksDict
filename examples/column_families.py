import shutil

from speedict import Rdict, Options, SliceTransform, PlainTableFactoryOptions
import random

path = str("tmp")
cf1_name = str("cf1")
cf2_name = str("cf2")

# set cf2 as a plain table
cf2_opt = Options()
cf2_opt.set_prefix_extractor(SliceTransform.create_max_len_prefix(8))
p_opt = PlainTableFactoryOptions()
p_opt.user_key_length = 200
cf2_opt.set_plain_table_factory(p_opt)

# create column families if missing
opt = Options() # create_if_missing=True by default
opt.create_missing_column_families(True)
db = Rdict(path, options=opt, column_families={cf1_name: Options(),
                                               cf2_name: cf2_opt})

# add column families
db_cf1 = db.get_column_family(cf1_name)
db_cf2 = db.get_column_family(cf2_name)
db_cf3 = db.create_column_family(str("cf3")) # with default Options
db_cf4 = db.create_column_family(str("cf4"), cf2_opt) # custom options

# remove column families
db.drop_column_family(str("cf3"))
db.drop_column_family(str("cf4"))
del db_cf3, db_cf4

# insert into column families
for i in range(10000):
    db_cf1[i] = i ** 2

rand_bytes = [random.randbytes(200) for _ in range(100000)]
for b in rand_bytes:
    db_cf2[b] = b

# close database
db_cf1.close()
db_cf2.close()
db.close()

assert cf1_name in Rdict.list_cf(path)
assert cf2_name in Rdict.list_cf(path)

# reopen db
# automatic reloading column families and options
db = Rdict(path)
db_cf1 = db.get_column_family(cf1_name)
db_cf2 = db.get_column_family(cf2_name)

# check keys
count = 0
for k, v in db_cf1.items():
    assert k == count
    assert v == count ** 2
    count += 1

assert count == 10000

rand_bytes.sort()
assert list(db_cf2.keys()) == rand_bytes

# delete db
db.close()
db_cf1.close()
db_cf2.close()
Rdict.destroy(path)
