import shutil

import rocksdict as rd
from tqdm import tqdm
import os


# create an option
opt = rd.Options()
opt.set_max_background_jobs(4)
opt.set_write_buffer_size(1024 * 1024 * 256)


# write 100000000 key-value pairs to external Sst
file_no = 1
file_name = lambda no: f"file_{no}.sst"
wt = rd.SstFileWriter(opt)
wt.open(file_name(file_no))
for i in tqdm(range(100000000)):
    wt[i] = i ** 2
    if wt.file_size() > 100 * 1024 * 1024:
        wt.finish()
        file_no += 1
        wt = rd.SstFileWriter(opt)
        wt.open(file_name(file_no))

wt.finish()


# bulk ingest
db = rd.Rdict("test", opt)
db.ingest_external_file([f for f in os.listdir(".") if f.endswith("sst")])
db.flush()

# check value
count = 0
for k, v in db.items():
    assert k == count
    assert v == count ** 2
    count += 1

# auto flush
del db
rd.Rdict.destroy("test")
shutil.rmtree("test")
