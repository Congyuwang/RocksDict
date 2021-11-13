import unittest
from rocksdict import Rdict
from random import randint, random, randbytes


TEST_INT_RANGE_UPPER = 999999


def compare_int_dicts(ref_dict: dict, test_dict: Rdict, lower: int, upper: int):
    # assert that the keys are the same
    for i in range(lower, upper):
        assert (i in ref_dict) == (i in test_dict)

    # assert that the values are the same
    for k, v in ref_dict.items():
        assert k in test_dict
        assert test_dict[k] == v


def compare_dicts(ref_dict: dict, test_dict: Rdict):
    # assert that the values are the same
    for k, v in ref_dict.items():
        assert k in test_dict
        assert test_dict[k] == v


class TestInt(unittest.TestCase):
    test_dict = None
    ref_dict = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.test_dict = Rdict("./temp_int")
        cls.ref_dict = dict()

    def test_add_integer(self):
        for i in range(10000):
            key = randint(0, TEST_INT_RANGE_UPPER)
            value = randint(0, TEST_INT_RANGE_UPPER)
            self.ref_dict[key] = value
            self.test_dict[key] = value

        compare_int_dicts(self.ref_dict, self.test_dict, 0, TEST_INT_RANGE_UPPER)

    def test_delete_integer(self):
        for i in range(5000):
            key = randint(0, TEST_INT_RANGE_UPPER)
            if key in self.ref_dict:
                del self.ref_dict[key]
                del self.test_dict[key]

        compare_int_dicts(self.ref_dict, self.test_dict, 0, TEST_INT_RANGE_UPPER)

    @classmethod
    def tearDownClass(cls):
        cls.test_dict.destroy()


class TestFloat(unittest.TestCase):
    test_dict = None
    ref_dict = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.test_dict = Rdict("./temp_float")
        cls.ref_dict = dict()

    def test_add_float(self):
        for i in range(10000):
            key = random()
            value = random()
            self.ref_dict[key] = value
            self.test_dict[key] = value

        compare_dicts(self.ref_dict, self.test_dict)

    def test_delete_float(self):
        for i in range(5000):
            keys = [k for k in self.ref_dict.keys()]
            key = keys[randint(0, len(self.ref_dict) - 1)]
            del self.ref_dict[key]
            del self.test_dict[key]

        compare_dicts(self.ref_dict, self.test_dict)

    @classmethod
    def tearDownClass(cls):
        cls.test_dict.destroy()


class TestBytes(unittest.TestCase):
    test_dict = None
    ref_dict = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.test_dict = Rdict("./temp_bytes")
        cls.ref_dict = dict()

    def test_add_bytes(self):
        for i in range(10000):
            key = randbytes(10)
            value = randbytes(20)
            self.ref_dict[key] = value
            self.test_dict[key] = value

        compare_dicts(self.ref_dict, self.test_dict)

    def test_delete_bytes(self):
        for i in range(5000):
            keys = [k for k in self.ref_dict.keys()]
            key = keys[randint(0, len(self.ref_dict) - 1)]
            del self.ref_dict[key]
            del self.test_dict[key]

        compare_dicts(self.ref_dict, self.test_dict)

    @classmethod
    def tearDownClass(cls):
        cls.test_dict.destroy()


class TestString(unittest.TestCase):
    test_dict = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.test_dict = Rdict("./temp_string")

    def test_string(self):
        self.test_dict["Guangdong"] = "Shenzhen"
        self.test_dict["Sichuan"] = "Changsha"
        # overwrite
        self.test_dict["Sichuan"] = "Chengdu"
        self.test_dict["Beijing"] = "Beijing"
        del self.test_dict["Beijing"]

        # assertions
        assert "Beijing" not in self.test_dict
        assert self.test_dict["Sichuan"] == "Chengdu"
        assert self.test_dict["Guangdong"] == "Shenzhen"

    @classmethod
    def tearDownClass(cls):
        cls.test_dict.destroy()


if __name__ == '__main__':
    unittest.main()
