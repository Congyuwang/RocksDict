import os.path
import timeit
import abc
from abc import ABC
from collections import deque
from rocksdict import Rdict, Mdict
from random import randint, random, getrandbits, uniform
import shutil
import pandas as pd
import dbm


# %%
def randbytes(n):
    """Generate n random bytes."""
    return getrandbits(n * 8).to_bytes(n, 'little')


class Test(metaclass=abc.ABCMeta):
    name = 'base'

    def __init__(self, key_size: int, data_size: int, num: int, place="./temp"):
        self.key_size = key_size
        self.data_size = data_size
        self.num = num
        self.rdict = Rdict(place)
        self.mdict = Mdict()
        self.pdict = dict()
        self.dbmdict = dbm.open('tmp.db', 'n')
        self.ref_dict = dict()

    def __str__(self):
        return '-' * 15 + f'This {self.__class__.name} test\'s key size is: {self.key_size} byte, data size is {self.data_size} byte, ' \
                          f'number of data is {self.num}' + '-' * 15

    def prepare_data(self):
        for i in range(self.num):
            self.ref_dict[randbytes(self.key_size)] = randbytes(self.data_size)

    @abc.abstractmethod
    def run(self, dict_type: str):
        pass

    def stop(self):
        self.rdict.destroy()
        self.ref_dict = None
        print('-' * 60)


class InsertTest(Test, ABC):
    name = 'InsertTest'

    def run(self, dict_type: str):
        if dict_type == 'r':
            for key, value in self.ref_dict.items():
                self.rdict[key] = value
        elif dict_type == 'm':
            for key, value in self.ref_dict.items():
                self.mdict[key] = value
        elif dict_type == 'p':
            for key, value in self.ref_dict.items():
                self.pdict[key] = value
        elif dict_type == 'dbm':
            for key, value in self.ref_dict.items():
                self.dbmdict[key] = value
        else:
            raise RuntimeError('Wrong dictionary type!')


class InsertDropTest(Test, ABC):
    name = 'InsertDropTest'

    def run(self, dict_type: str):
        if dict_type == 'r':
            for key, value in self.ref_dict.items():
                self.rdict[key] = value
            for key in self.ref_dict:
                del self.rdict[key]
        elif dict_type == 'm':
            for key, value in self.ref_dict.items():
                self.mdict[key] = value
            for key in self.ref_dict:
                del self.mdict[key]
        elif dict_type == 'p':
            for key, value in self.ref_dict.items():
                self.pdict[key] = value
            for key in self.ref_dict:
                del self.pdict[key]
        elif dict_type == 'dbm':
            for key, value in self.ref_dict.items():
                self.dbmdict[key] = value
            for key in self.ref_dict:
                del self.dbmdict[key]
        else:
            raise RuntimeError('Wrong dictionary type!')


class MixtureTest(Test, ABC):
    name = 'MixtureTest'

    def run(self, dict_type: str):
        temp_keys = deque()
        add = uniform(0, 1) >= 0.5
        if dict_type == 'r':
            for key, value in self.ref_dict.items():
                if add:
                    temp_keys.append(key)
                    self.rdict[key] = value
                else:
                    if temp_keys.__len__() > 0:
                        del self.rdict[temp_keys.popleft()]
        elif dict_type == 'm':
            for key, value in self.ref_dict.items():
                if add:
                    temp_keys.append(key)
                    self.mdict[key] = value
                else:
                    if temp_keys.__len__() > 0:
                        del self.mdict[temp_keys.popleft()]
        elif dict_type == 'p':
            for key, value in self.ref_dict.items():
                if add:
                    temp_keys.append(key)
                    self.pdict[key] = value
                else:
                    if temp_keys.__len__() > 0:
                        del self.pdict[temp_keys.popleft()]
        elif dict_type == 'dbm':
            for key, value in self.ref_dict.items():
                if add:
                    temp_keys.append(key)
                    self.dbmdict[key] = value
                else:
                    if temp_keys.__len__() > 0:
                        del self.dbmdict[temp_keys.popleft()]
        else:
            raise RuntimeError('Wrong dictionary type!')


class PressureInsertTest(Test, ABC):
    name = 'PressureInsertTest'

    def prepare_data(self, pressure=100000, dict_type: str = 'r'):
        if dict_type == 'r':
            for j in range(pressure):
                self.rdict[randbytes(self.key_size)] = randbytes(self.data_size)
        elif dict_type == 'm':
            for j in range(pressure):
                self.mdict[randbytes(self.key_size)] = randbytes(self.data_size)
        elif dict_type == 'p':
            for j in range(pressure):
                self.pdict[randbytes(self.key_size)] = randbytes(self.data_size)
        elif dict_type == 'dbm':
            for j in range(pressure):
                self.dbmdict[randbytes(self.key_size)] = randbytes(self.data_size)
        else:
            raise RuntimeError('Wrong dictionary type!')
        for i in range(self.num):
            self.ref_dict[randbytes(self.key_size)] = randbytes(self.data_size)

    def run(self, dict_type: str):
        if dict_type == 'r':
            for key, value in self.ref_dict.items():
                self.rdict[key] = value
        elif dict_type == 'm':
            for key, value in self.ref_dict.items():
                self.mdict[key] = value
        elif dict_type == 'p':
            for key, value in self.ref_dict.items():
                self.pdict[key] = value
        elif dict_type == 'dbm':
            for key, value in self.ref_dict.items():
                self.dbmdict[key] = value


def clear_dir(path='./temp'):
    shutil.rmtree(path)
    print('-' * 60)


def main(data_num: int = 1000, repeat: int = 10):
    d_size_list = []
    k_size_list = []
    insert_list = []
    insert_drop_list = []
    mix_list = []
    pressure_list = []
    d_type_list = []
    for d_size in [64, 256, 1024]:
        for k_size in [16, 128]:
            for d_type in ['r', 'm', 'p', 'dbm']:
                d_size_list.append(d_size)
                k_size_list.append(k_size)
                d_type_list.append(d_type_list)
                t = timeit.timeit(f"insert_test.run(dict_type='{d_type}')",
                                  setup=f"from __main__ import randbytes, Test, InsertTest; insert_test = InsertTest({k_size}, {d_size}, {data_num}); print(insert_test); insert_test.prepare_data()",
                                  number=repeat)
                print(f'The time usage is {t / repeat} seconds.')
                clear_dir()
                insert_list.append(t)
                t = timeit.timeit(f"insertdrop_test.run(dict_type='{d_type}')",
                                  setup=f"from __main__ import  InsertDropTest; insertdrop_test = InsertDropTest({k_size}, {d_size}, {data_num}); print(insertdrop_test); insertdrop_test.prepare_data()",
                                  number=repeat)
                print(f'The time usage is {t / repeat} seconds.')
                clear_dir()
                insert_drop_list.append(t)
                t = timeit.timeit(f"mix_test.run(dict_type='{d_type}')",
                                  setup=f"from __main__ import  MixtureTest; mix_test = MixtureTest({k_size}, {d_size}, {data_num}); print(mix_test); mix_test.prepare_data()",
                                  number=repeat)
                print(f'The time usage is {t / repeat} seconds.')
                clear_dir()
                mix_list.append(t)
                t = timeit.timeit(f"pressureinsert_test.run(dict_type='{d_type}')",
                                  setup=f"from __main__ import  PressureInsertTest; pressureinsert_test = PressureInsertTest({k_size}, {d_size}, {data_num}); print(pressureinsert_test); pressureinsert_test.prepare_data(dict_type='{d_type}')",
                                  number=repeat)
                print(f'The time usage is {t / repeat} seconds.')
                clear_dir()
                pressure_list.append(t)
    df = pd.DataFrame({'dict_type': d_type_list, 'data_size': d_size_list, 'key_size': k_size_list,
                       'insert_time': insert_list, 'insert_drop_time': insert_drop_list, 'mixture_time': mix_list,
                       'pressure_insert_time': pressure_list})
    df.to_csv('./result.csv')


if __name__ == "__main__":
    main()
