from rocksdict import Rdict, WriteBatch, Options

opt = Options(True)
path = "./temp_write_batch_wide_columns_raw"
opt.create_if_missing(True)
test_dict = Rdict(path, opt)

write_batch = WriteBatch(raw_mode=True)
default_cf_handle = test_dict.get_column_family_handle("default")
# Cannot call `put_entity` without a column family handle
write_batch.set_default_column_family(default_cf_handle)

write_batch.put_entity(key=b"Guangdong", names=[b"language", b"city"], values=[b"Cantonese", b"Shenzhen"])
write_batch.put_entity(key=b"Sichuan", names=[b"language", b"city"], values=[b"Mandarin", b"Chengdu"])
write_batch.put_entity(key=b"Sichuan", names=[b"language", b"city"], values=[b"Sichuanhua", b"Chengdu"])
write_batch[b"Beijing"] = b"Beijing"

# write batch
test_dict.write(write_batch)

# assertions
assert test_dict[b"Beijing"] == b"Beijing"
assert test_dict.get_entity(b"Beijing") == [(b"", b"Beijing")]
assert test_dict[b"Guangdong"] == b""
assert test_dict.get_entity(b"Guangdong") == [(b"city", b"Shenzhen"), (b"language", b"Cantonese")]
assert test_dict[b"Sichuan"] == b""
assert test_dict.get_entity(b"Sichuan") == [(b"city", b"Chengdu"), (b"language", b"Sichuanhua")]

# iterator
it = test_dict.iter()
it.seek_to_first()
assert it.valid()
assert it.key() == b"Beijing"
assert it.value() == b"Beijing"
assert it.columns() == [(b"", b"Beijing")]
it.next()
assert it.valid()
assert it.key() == b"Guangdong"
assert it.value() == b""
assert it.columns() == [(b"city", b"Shenzhen"), (b"language", b"Cantonese")]
it.next()
assert it.valid()
assert it.key() == b"Sichuan"
assert it.value() == b""
assert it.columns() == [(b"city", b"Chengdu"), (b"language", b"Sichuanhua")]

# iterators
expected = [
    (b"Beijing", [(b"", b"Beijing")]),
    (b"Guangdong", [(b"city", b"Shenzhen"), (b"language", b"Cantonese")]),
    (b"Sichuan", [(b"city", b"Chengdu"), (b"language", b"Sichuanhua")]),
]
for i, (key, entity) in enumerate(test_dict.entities()):
    assert key == expected[i][0]
    assert entity == expected[i][1]

assert (
    [c for c in test_dict.columns()] == [
        [(b"", b"Beijing")],
        [(b"city", b"Shenzhen"), (b"language", b"Cantonese")],
        [(b"city", b"Chengdu"), (b"language", b"Sichuanhua")],
    ]
)

del write_batch, it, default_cf_handle, test_dict
Rdict.destroy(path, opt)
