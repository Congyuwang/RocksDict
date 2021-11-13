import unittest
from rocksdict import Rdict
from random import randint


TEST_INT_RANGE_UPPER = 9999999


def compare_int_dicts(ref_dict: dict, test_dict: Rdict, lower: int, upper: int):
    # assert that the keys are the same
    for i in range(lower, upper):
        assert (i in ref_dict) == (i.to_bytes(4, "little") in test_dict)

    # assert that the values are the same
    for k, v in ref_dict.items():
        assert k.to_bytes(4, "little") in test_dict
        assert int.from_bytes(test_dict[k.to_bytes(4, "little")], "little")


class TestIntKeys(unittest.TestCase):
    test_dict = None
    ref_dict = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.test_dict = Rdict("./temp")
        cls.ref_dict = dict()

    def test_add_integer(self):
        for i in range(10000):
            key = randint(0, TEST_INT_RANGE_UPPER)
            value = randint(0, TEST_INT_RANGE_UPPER)
            self.ref_dict[key] = value
            self.test_dict[key.to_bytes(4, "little")] = value.to_bytes(4, "little")

        compare_int_dicts(self.ref_dict, self.test_dict, 0, TEST_INT_RANGE_UPPER)

    def test_delete_integer(self):
        for i in range(5000):
            key = randint(0, TEST_INT_RANGE_UPPER)
            if key in self.ref_dict:
                del self.ref_dict[key]
                del self.test_dict[key.to_bytes(4, "little")]

        compare_int_dicts(self.ref_dict, self.test_dict, 0, TEST_INT_RANGE_UPPER)

    @classmethod
    def tearDownClass(cls):
        cls.test_dict.destroy()


class TestStringKeys(unittest.TestCase):
    test_dict = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.test_dict = Rdict("./temp2")

    def test_string(self):
        self.test_dict[b"Guangdong"] = b"Shenzhen"
        self.test_dict[b"Sichuan"] = b"Changsha"
        # overwrite
        self.test_dict[b"Sichuan"] = b"Chengdu"
        self.test_dict[b"Beijing"] = b"Beijing"
        del self.test_dict[b"Beijing"]

        # assertions
        assert b"Beijing" not in self.test_dict
        assert self.test_dict[b"Sichuan"] == b"Chengdu"
        assert self.test_dict[b"Guangdong"] == b"Shenzhen"

    @classmethod
    def tearDownClass(cls):
        cls.test_dict.destroy()


if __name__ == '__main__':
    unittest.main()
