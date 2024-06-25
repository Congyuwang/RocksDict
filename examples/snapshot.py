from rocksdict import Rdict

db = Rdict("tmp")
for i in range(100):
    db[i] = i

# take a snapshot
snapshot = db.snapshot()

for i in range(90):
    del db[i]

# 0-89 are no longer in db
for k, v in db.items():
    assert k is int
    assert v is int
    assert k >= 90
    assert v >= 90

# but they are still in the snapshot
for i in range(100):
    assert snapshot[i] == i

# drop the snapshot
del snapshot, db

Rdict.destroy("tmp")
