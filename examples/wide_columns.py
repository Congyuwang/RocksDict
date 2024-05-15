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
expected = [
    ("Beijing", [("", "Beijing")]),
    ("Guangdong", [("city", "Shenzhen"), ("language", "Cantonese"), ("population", 1.27)]),
    ("Sichuan", [("city", "Chengdu"), ("language", "Sichuanhua")]),
]
for i, (key, entity) in enumerate(test_dict.entities()):
    assert key == expected[i][0]
    assert entity == expected[i][1]

all_columns = [
    [("", "Beijing")],
    [("city", "Shenzhen"), ("language", "Cantonese"), ("population", 1.27)],
    [("city", "Chengdu"), ("language", "Sichuanhua")],
]
assert [c for c in test_dict.columns()] == all_columns

del test_dict

Rdict.destroy(path)
