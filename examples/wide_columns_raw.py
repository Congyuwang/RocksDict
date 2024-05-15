from rocksdict import Rdict, Options

test_dict = None
opt = None
path = "./temp_wide_columns_raw"

opt = Options(True)
opt.create_if_missing(True)
test_dict = Rdict(path, opt)

# write
test_dict.put_entity(key=b"Guangdong", names=[b"language", b"city"], values=[b"Cantonese", b"Shenzhen"]);
test_dict.put_entity(key=b"Sichuan", names=[b"language", b"city"], values=[b"Mandarin", b"Chengdu"]);

# read
assert test_dict[b"Guangdong"] == b""
assert test_dict.get_entity(b"Guangdong") == [(b"city", b"Shenzhen"), (b"language", b"Cantonese")]

assert test_dict[b"Sichuan"] == b""
assert test_dict.get_entity(b"Sichuan") == [(b"city", b"Chengdu"), (b"language", b"Mandarin")]

# overwrite
test_dict.put_entity(key=b"Sichuan", names=[b"language", b"city"], values=[b"Sichuanhua", b"Chengdu"]);
test_dict[b"Beijing"] = b"Beijing"

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

del it, test_dict

Rdict.destroy(path)
