
from rocksdict import Rdict, Mdict
from random import getrandbits, uniform
import shutil
import os
import dbm
import pytest


# %%
def randbytes(n):
    """Generate n random bytes."""
    return getrandbits(n * 8).to_bytes(n, 'little')


def clear_dir(path='./temp'):
    shutil.rmtree(path)


class TargetDict:
    def __init__(self, key_size: int, data_size: int, place='./', pressure_num=1000000):  # ./temp
        self.sample_path = os.path.join(place, 'temp')
        self.pressure_path = os.path.join(place, 'pressure_temp')
        self.sample_dbm = os.path.join(place, 'tmp.db')
        self.pressure_dbm = os.path.join(place, 'pressure_tmp.db')
        self.key_size = key_size
        self.data_size = data_size
        self.pressure_num = pressure_num
        self.r_dict = Rdict(self.sample_path)
        self.pressure_r_dict = Rdict(self.pressure_path)
        self.m_dict = Mdict()
        self.pressure_m_dict = Mdict()
        self.p_dict = dict()
        self.pressure_p_dict = dict()
        self.dbm_dict = dbm.open(self.sample_dbm, 'n')
        self.pressure_dbm_dict = dbm.open(self.pressure_dbm, 'n')
        self.ref_dict = dict()
        self.backup_dict = dict()

    def prepare_ref_data(self, ref_num=2000000, dict_type='p'):
        if dict_type == 'p':
            for i in range(ref_num * 5):
                self.ref_dict[randbytes(self.key_size)] = randbytes(self.data_size)
                self.backup_dict[randbytes(self.key_size)] = randbytes(self.data_size)
        else:
            for i in range(ref_num):
                self.ref_dict[randbytes(self.key_size)] = randbytes(self.data_size)
                self.backup_dict[randbytes(self.key_size)] = randbytes(self.data_size)

    def prepare_data(self, dict_type: str = 'r'):
        if self.ref_dict.__len__() == 0:
            raise RuntimeError
        elif dict_type == 'r':
            for key, value in self.ref_dict.items():
                self.r_dict[key] = value
        elif dict_type == 'm':
            for key, value in self.ref_dict.items():
                self.m_dict[key] = value
        elif dict_type == 'p':
            for key, value in self.ref_dict.items():
                self.p_dict[key] = value
        elif dict_type == 'dbm':
            for key, value in self.ref_dict.items():
                self.dbm_dict[key] = value
        else:
            raise RuntimeError('Wrong dictionary type!')

    def prepare_pressure_data(self, dict_type: str = 'r'):
        if dict_type == 'r':
            for i in range(self.pressure_num):
                self.pressure_r_dict[randbytes(self.key_size)] = randbytes(self.data_size)
        elif dict_type == 'm':
            for i in range(self.pressure_num):
                self.pressure_m_dict[randbytes(self.key_size)] = randbytes(self.data_size)
        elif dict_type == 'p':
            for i in range(self.pressure_num):
                self.pressure_p_dict[randbytes(self.key_size)] = randbytes(self.data_size)
        elif dict_type == 'dbm':
            for i in range(self.pressure_num):
                self.pressure_dbm_dict[randbytes(self.key_size)] = randbytes(self.data_size)
        else:
            raise RuntimeError('Wrong dictionary type!')

    def clear_data(self):
        self.r_dict.destroy()
        # clear_dir(self.sample_dbm + '.dat')
        # clear_dir(self.sample_dbm + '.dir')

    def clear_pressure_data(self):
        self.pressure_r_dict.destroy()
        # clear_dir(self.pressure_dbm + '.dat')
        # clear_dir(self.pressure_dbm + '.dir')


def insert(tar_dict: TargetDict, dict_type: str = 'r'):
    key, value = tar_dict.ref_dict.popitem()
    if dict_type == 'r':
        tar_dict.r_dict[key] = value
    elif dict_type == 'm':
        tar_dict.m_dict[key] = value
    elif dict_type == 'p':
        tar_dict.p_dict[key] = value
    elif dict_type == 'dbm':
        tar_dict.dbm_dict[key] = value
    else:
        raise RuntimeError('Wrong dictionary type!')


def insert_drop(tar_dict: TargetDict, dict_type: str = 'r'):
    key, value = tar_dict.ref_dict.popitem()
    b_key, b_value = tar_dict.backup_dict.popitem()
    if dict_type == 'r':
        tar_dict.r_dict[b_key] = b_value
        del tar_dict.r_dict[key]
    elif dict_type == 'm':
        tar_dict.m_dict[b_key] = b_value
        del tar_dict.m_dict[key]
    elif dict_type == 'p':
        tar_dict.p_dict[b_key] = b_value
        del tar_dict.p_dict[key]
    elif dict_type == 'dbm':
        tar_dict.dbm_dict[b_key] = b_value
        del tar_dict.dbm_dict[key]
    else:
        raise RuntimeError('Wrong dictionary type!')


def mixture(tar_dict: TargetDict, dict_type: str = 'r'):
    key, value = tar_dict.ref_dict.popitem()
    b_key, b_value = tar_dict.backup_dict.popitem()
    add = uniform(0, 1) >= 0.5
    if dict_type == 'r':
        if add:
            tar_dict.r_dict[b_key] = b_value
        else:
            del tar_dict.r_dict[key]
    elif dict_type == 'm':
        if add:
            tar_dict.m_dict[b_key] = b_value
        else:
            del tar_dict.m_dict[key]
    elif dict_type == 'p':
        if add:
            tar_dict.p_dict[b_key] = b_value
        else:
            del tar_dict.p_dict[key]
    elif dict_type == 'dbm':
        if add:
            tar_dict.dbm_dict[b_key] = b_value
        else:
            del tar_dict.dbm_dict[key]
    else:
        raise RuntimeError('Wrong dictionary type!')


def pressure_insert(tar_dict: TargetDict, dict_type: str = 'r'):
    key, value = tar_dict.ref_dict.popitem()
    if dict_type == 'r':
        tar_dict.pressure_r_dict[key] = value
    elif dict_type == 'm':
        tar_dict.pressure_m_dict[key] = value
    elif dict_type == 'p':
        tar_dict.pressure_p_dict[key] = value
    elif dict_type == 'dbm':
        tar_dict.pressure_dbm_dict[key] = value
    else:
        raise RuntimeError('Wrong dictionary type!')


# -----------------------------------------------
def test_insert_key4_data4_Rdict(benchmark):
    target_dict = TargetDict(4, 4)
    target_dict.prepare_ref_data(dict_type='n')
    benchmark(insert, target_dict, 'r')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_insert_key4_data4_Mdict(benchmark):
    target_dict = TargetDict(4, 4)
    target_dict.prepare_ref_data(dict_type='n')
    benchmark(insert, target_dict, 'm')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_insert_key8_data128_Rdict(benchmark):
    target_dict = TargetDict(8, 128)
    target_dict.prepare_ref_data(dict_type='n')
    benchmark(insert, target_dict, 'r')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_insert_key8_data128_Mdict(benchmark):
    target_dict = TargetDict(8, 128)
    target_dict.prepare_ref_data(dict_type='n')
    benchmark(insert, target_dict, 'm')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_insert_key16_data1024_Rdict(benchmark):
    target_dict = TargetDict(16, 1024)
    target_dict.prepare_ref_data(dict_type='n')
    benchmark(insert, target_dict, 'r')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_insert_key16_data1024_Mdict(benchmark):
    target_dict = TargetDict(16, 1024)
    target_dict.prepare_ref_data(dict_type='n')
    benchmark(insert, target_dict, 'm')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_insert_key128_data16_Rdict(benchmark):
    target_dict = TargetDict(128, 16)
    target_dict.prepare_ref_data(dict_type='n')
    benchmark(insert, target_dict, 'r')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_insert_key128_data16_Mdict(benchmark):
    target_dict = TargetDict(128, 16)
    target_dict.prepare_ref_data(dict_type='n')
    benchmark(insert, target_dict, 'm')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_insert_key128_data16_Pdict(benchmark):
    target_dict = TargetDict(128, 16)
    target_dict.prepare_ref_data(dict_type='p')
    benchmark(insert, target_dict, 'p')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


# def test_insert_key128_data16_DBMdict(benchmark):
#     target_dict = TargetDict(128, 16)
#     target_dict.prepare_ref_data(dict_type='n')
#     benchmark(insert, target_dict, 'dbm')
#     target_dict.clear_data()
#     target_dict.clear_pressure_data()


# -----------------------------------------
def test_insert_drop_key4_data4_Rdict(benchmark):
    target_dict = TargetDict(4, 4)
    target_dict.prepare_ref_data(dict_type='n')
    target_dict.prepare_data('r')
    benchmark(insert_drop, target_dict, 'r')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_insert_drop_key4_data4_Mdict(benchmark):
    target_dict = TargetDict(4, 4)
    target_dict.prepare_ref_data(dict_type='n')
    target_dict.prepare_data('m')
    benchmark(insert_drop, target_dict, 'm')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_insert_drop_key8_data128_Rdict(benchmark):
    target_dict = TargetDict(8, 128)
    target_dict.prepare_ref_data(dict_type='n')
    target_dict.prepare_data('r')
    benchmark(insert_drop, target_dict, 'r')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_insert_drop_key8_data128_Mdict(benchmark):
    target_dict = TargetDict(8, 128)
    target_dict.prepare_ref_data(dict_type='n')
    target_dict.prepare_data('m')
    benchmark(insert_drop, target_dict, 'm')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_insert_drop_key16_data1024_Rdict(benchmark):
    target_dict = TargetDict(16, 1024)
    target_dict.prepare_ref_data(dict_type='n')
    target_dict.prepare_data('r')
    benchmark(insert_drop, target_dict, 'r')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_insert_drop_key16_data1024_Mdict(benchmark):
    target_dict = TargetDict(16, 1024)
    target_dict.prepare_ref_data(dict_type='n')
    target_dict.prepare_data('m')
    benchmark(insert_drop, target_dict, 'm')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_insert_drop_key128_data16_Rdict(benchmark):
    target_dict = TargetDict(128, 16)
    target_dict.prepare_ref_data(dict_type='n')
    target_dict.prepare_data('r')
    benchmark(insert_drop, target_dict, 'r')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_insert_drop_key128_data16_Mdict(benchmark):
    target_dict = TargetDict(128, 16)
    target_dict.prepare_ref_data(dict_type='n')
    target_dict.prepare_data('m')
    benchmark(insert_drop, target_dict, 'm')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_insert_drop_key128_data16_Pdict(benchmark):
    target_dict = TargetDict(128, 16)
    target_dict.prepare_ref_data(dict_type='p')
    target_dict.prepare_data('p')
    benchmark(insert_drop, target_dict, 'p')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


# def test_insert_drop_key128_data16_DBMdict(benchmark):
#     target_dict = TargetDict(128, 16)
#     target_dict.prepare_ref_data(dict_type='n')
#     target_dict.prepare_data('dbm')
#     benchmark(insert_drop, target_dict, 'dbm')
#     target_dict.clear_data()
#     target_dict.clear_pressure_data()


# ----------------------------------------------------
def test_mixture_key4_data4_Rdict(benchmark):
    target_dict = TargetDict(4, 4)
    target_dict.prepare_ref_data(dict_type='n')
    target_dict.prepare_data('r')
    benchmark(mixture, target_dict, 'r')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_mixture_key4_data4_Mdict(benchmark):
    target_dict = TargetDict(4, 4)
    target_dict.prepare_ref_data(dict_type='n')
    target_dict.prepare_data('m')
    benchmark(mixture, target_dict, 'm')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_mixture_key8_data128_Rdict(benchmark):
    target_dict = TargetDict(8, 128)
    target_dict.prepare_ref_data(dict_type='n')
    target_dict.prepare_data('r')
    benchmark(mixture, target_dict, 'r')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_mixture_key8_data128_Mdict(benchmark):
    target_dict = TargetDict(8, 128)
    target_dict.prepare_ref_data(dict_type='n')
    target_dict.prepare_data('m')
    benchmark(mixture, target_dict, 'm')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_mixture_key16_data1024_Rdict(benchmark):
    target_dict = TargetDict(16, 1024)
    target_dict.prepare_ref_data(dict_type='n')
    target_dict.prepare_data('r')
    benchmark(mixture, target_dict, 'r')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_mixture_key16_data1024_Mdict(benchmark):
    target_dict = TargetDict(16, 1024)
    target_dict.prepare_ref_data(dict_type='n')
    target_dict.prepare_data('m')
    benchmark(mixture, target_dict, 'm')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_mixture_key128_data16_Rdict(benchmark):
    target_dict = TargetDict(128, 16)
    target_dict.prepare_ref_data(dict_type='n')
    target_dict.prepare_data('r')
    benchmark(mixture, target_dict, 'r')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_mixture_key128_data16_Mdict(benchmark):
    target_dict = TargetDict(128, 16)
    target_dict.prepare_ref_data(dict_type='n')
    target_dict.prepare_data('m')
    benchmark(mixture, target_dict, 'm')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_mixture_key128_data16_Pdict(benchmark):
    target_dict = TargetDict(128, 16)
    target_dict.prepare_ref_data(dict_type='p')
    target_dict.prepare_data('p')
    benchmark(mixture, target_dict, 'p')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


# def test_mixture_key128_data16_DBMdict(benchmark):
#     target_dict = TargetDict(128, 16)
#     target_dict.prepare_ref_data(dict_type='n')
#     target_dict.prepare_data('dbm')
#     benchmark(mixture, target_dict, 'dbm')
#     target_dict.clear_data()
#     target_dict.clear_pressure_data()


# ------------------------------------------------
def test_pressure_insert_key4_data4_Rdict(benchmark):
    target_dict = TargetDict(4, 4)
    target_dict.prepare_ref_data(dict_type='n')
    target_dict.prepare_pressure_data('r')
    benchmark(pressure_insert, target_dict, 'r')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_pressure_insert_key4_data4_Mdict(benchmark):
    target_dict = TargetDict(4, 4)
    target_dict.prepare_ref_data(dict_type='n')
    target_dict.prepare_pressure_data('m')
    benchmark(pressure_insert, target_dict, 'm')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_pressure_insert_key8_data128_Rdict(benchmark):
    target_dict = TargetDict(8, 128)
    target_dict.prepare_ref_data(dict_type='n')
    target_dict.prepare_pressure_data('r')
    benchmark(pressure_insert, target_dict, 'r')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_pressure_insert_key8_data128_Mdict(benchmark):
    target_dict = TargetDict(8, 128)
    target_dict.prepare_ref_data(dict_type='n')
    target_dict.prepare_pressure_data('m')
    benchmark(pressure_insert, target_dict, 'm')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_pressure_insert_key16_data1024_Rdict(benchmark):
    target_dict = TargetDict(16, 1024)
    target_dict.prepare_ref_data(dict_type='n')
    target_dict.prepare_pressure_data('r')
    benchmark(pressure_insert, target_dict, 'r')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_pressure_insert_key16_data1024_Mdict(benchmark):
    target_dict = TargetDict(16, 1024)
    target_dict.prepare_ref_data(dict_type='n')
    target_dict.prepare_pressure_data('m')
    benchmark(pressure_insert, target_dict, 'm')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_pressure_insert_key128_data16_Rdict(benchmark):
    target_dict = TargetDict(128, 16)
    target_dict.prepare_ref_data(dict_type='n')
    target_dict.prepare_pressure_data('r')
    benchmark(pressure_insert, target_dict, 'r')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_pressure_insert_key128_data16_Mdict(benchmark):
    target_dict = TargetDict(128, 16)
    target_dict.prepare_ref_data(dict_type='n')
    target_dict.prepare_pressure_data('m')
    benchmark(pressure_insert, target_dict, 'm')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


def test_pressure_insert_key128_data16_Pdict(benchmark):
    target_dict = TargetDict(128, 16)
    target_dict.prepare_ref_data(dict_type='p')
    target_dict.prepare_pressure_data('p')
    benchmark(pressure_insert, target_dict, 'p')
    target_dict.clear_data()
    target_dict.clear_pressure_data()


# def test_pressure_insert_key128_data16_DBMdict(benchmark):
#     target_dict = TargetDict(128, 16)
#     target_dict.prepare_ref_data(dict_type='n')
#     target_dict.prepare_pressure_data('dbm')
#     benchmark(pressure_insert, target_dict, 'dbm')
#     target_dict.clear_data()
#     target_dict.clear_pressure_data()
