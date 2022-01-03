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
    print(f"{k} -> {v}")

# but they are still in the snapshot
for i in range(100):
    assert snapshot[i] == i

count = 0
for k, v in snapshot.items():
    assert k == count
    assert v == count
    count += 1

# drop the snapshot
del snapshot, db

Rdict.destroy("tmp")
