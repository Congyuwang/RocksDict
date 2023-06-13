import shutil

from speedict import Rdict, Options

path = str("tmp")
c1_name = str("c1")

db = Rdict(path)
c1 = db.create_column_family(c1_name, Options())

# write keys
for i in range(0, 100):
    db[i] = i
    c1[i] = i

# delete range
db.delete_range(0, 50)
c1.delete_range(50, 100)

# check keys after delete_range
assert list(db.keys()) == list(range(50, 100))
assert list(c1.keys()) == list(range(0, 50))

c1.close()
db.close()
Rdict.destroy(path)
