from rocksdict import Rdict, Options

test_dict = None
opt = None
path = "./temp_wide_columns"

opt = Options()
opt.create_if_missing(True)
test_dict = Rdict(path, opt)

# write
test_dict.put_entity(key="Guangdong", names=["population", "language", "city"], values=[1.27, "Cantonese", "Shenzhen"]);
test_dict.put_entity(key="Sichuan", names=["language", "city"], values=["Mandarin", "Chengdu"]);

# read
assert test_dict["Guangdong"] == ""
assert test_dict.get_entity("Guangdong") == [("city", "Shenzhen"), ("language", "Cantonese"), ("population", 1.27)]

assert test_dict["Sichuan"] == ""
assert test_dict.get_entity("Sichuan") == [("city", "Chengdu"), ("language", "Mandarin")]

# overwrite
test_dict.put_entity(key="Sichuan", names=["language", "city"], values=["Sichuanhua", "Chengdu"]);
test_dict["Beijing"] = "Beijing"

# assertions
assert test_dict["Beijing"] == "Beijing"
assert test_dict.get_entity("Beijing") == [("", "Beijing")]

assert test_dict["Guangdong"] == ""
assert test_dict.get_entity("Guangdong") == [("city", "Shenzhen"), ("language", "Cantonese"), ("population", 1.27)]

assert test_dict["Sichuan"] == ""
assert test_dict.get_entity("Sichuan") == [("city", "Chengdu"), ("language", "Sichuanhua")]

# iterator
it = test_dict.iter()
it.seek_to_first()

assert it.valid()
assert it.key() == "Beijing"
assert it.value() == "Beijing"
assert it.columns() == [("", "Beijing")]

it.next()
assert it.valid()
assert it.key() == "Guangdong"
assert it.value() == ""
assert it.columns() == [("city", "Shenzhen"), ("language", "Cantonese"), ("population", 1.27)]

it.next()
assert it.valid()
assert it.key() == "Sichuan"
assert it.value() == ""
assert it.columns() == [("city", "Chengdu"), ("language", "Sichuanhua")]

del it, test_dict

Rdict.destroy(path)
