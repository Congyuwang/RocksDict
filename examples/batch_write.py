from rocksdict import Rdict, WriteBatch, Options

# create db with two new column families
path = str("tmp")
opt = Options()
opt.create_missing_column_families(True)
cf_name_1 = str("batch_test_1")
cf_name_2 = str("batch_test_2")
cf = {cf_name_1: Options(), cf_name_2: Options()}
db = Rdict(path, column_families=cf, options=opt)

# write batch to ColumnFamily `batch_test_1` (method 1)
wb = WriteBatch()
for i in range(100):
    wb.put(i, i**2, db.get_column_family_handle(cf_name_1))

db.write(wb)

# write batch to ColumnFamily `batch_test_2` (method 2, change default cf)
wb = WriteBatch()
wb.set_default_column_family(db.get_column_family_handle(cf_name_2))
for i in range(100, 200):
    wb[i] = i**2

db.write(wb)

# reopen DB
db.close()

# automatic reloading column families
db = Rdict(path)

# read db, check elements in two column families
count = 0
for k, v in db.get_column_family(cf_name_1).items():
    assert k == count
    assert v == count**2
    count += 1

assert count == 100

for k, v in db.get_column_family(cf_name_2).items():
    assert k == count
    assert v == count**2
    count += 1

assert count == 200

db.close()
Rdict.destroy(path, opt)
