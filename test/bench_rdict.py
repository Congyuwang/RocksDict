from rocksdict import Rdict, Options
from random import randbytes
from threading import Thread
import time


def gen_rand_bytes():
    return [randbytes(128) for i in range(1024 * 1024)]


def perf_put_single_thread(rand_bytes):
    rdict = Rdict('test.db', Options(raw_mode=True))
    start = time.time()
    for k in rand_bytes:
        rdict[k] = k
    end = time.time()
    print('Put performance: {} items in {} seconds'.format(len(rand_bytes), end - start))
    count = 0
    for k, v in rdict.items():
        assert k == v
        count += 1
    assert count == len(rand_bytes)
    rdict.close()
    Rdict.destroy('test.db')


def perf_put_multi_thread(rand_bytes):
    rdict = Rdict('test.db', Options(raw_mode=True))
    start = time.time()
    THREAD = 4
    def perf_put(dat):
        for k in dat:
            rdict[k] = k
    threads = []
    each_len = len(rand_bytes) // THREAD
    for i in range(THREAD):
        t = Thread(target=perf_put, args=(rand_bytes[i*each_len:(i+1)*each_len],))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    end = time.time()
    print('Put performance multi-thread: {} items in {} seconds'.format(len(rand_bytes), end - start))
    count = 0
    for k, v in rdict.items():
        assert k == v
        count += 1
    assert count == len(rand_bytes)
    rdict.close()
    Rdict.destroy('test.db')


def perf_iterator_single_thread(rand_bytes):
    rdict = Rdict('test.db', Options(raw_mode=True))
    start = time.time()
    count = 0
    for k, v in rdict.items():
        assert k == v
        count += 1
    end = time.time()
    assert count == len(rand_bytes)
    print('Iterator performance: {} items in {} seconds'.format(count, end - start))
    rdict.close()


def perf_iterator_multi_thread(rand_bytes):
    rdict = Rdict('test.db', Options(raw_mode=True))
    start = time.time()
    THREAD = 4
    def perf_iter():
        count = 0
        for k, v in rdict.items():
            assert k == v
            count += 1
        assert count == len(rand_bytes)
    threads = []
    for _ in range(THREAD):
        t = Thread(target=perf_iter)
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    end = time.time()
    print('Iterator performance multi-thread: {} items in {} seconds'.format(THREAD * len(rand_bytes), end - start))
    rdict.close()


def perf_random_get_single_thread(rand_bytes):
    rdict = Rdict('test.db', Options(raw_mode=True))
    start = time.time()
    for k in rand_bytes:
        assert k == rdict[k]
    end = time.time()
    print('Get performance: {} items in {} seconds'.format(len(rand_bytes), end - start))
    rdict.close()


def perf_random_get_multi_thread(rand_bytes):
    rdict = Rdict('test.db', Options(raw_mode=True))
    start = time.time()
    THREAD = 4
    def perf_get(dat):
        for k in dat:
            assert k == rdict[k]
    threads = []
    each_len = len(rand_bytes) // THREAD
    for i in range(THREAD):
        t = Thread(target=perf_get, args=(rand_bytes[i*each_len:(i+1)*each_len],))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    end = time.time()
    print('Get performance multi-thread: {} items in {} seconds'.format(len(rand_bytes), end - start))
    rdict.close()


if __name__ == '__main__':
    print('Gen rand bytes...')
    rand_bytes = gen_rand_bytes()

    print('Benchmarking Rdict Put...')
    # perf write
    perf_put_single_thread(rand_bytes)
    perf_put_multi_thread(rand_bytes)

    # Create a new Rdict instance
    rdict = Rdict('test.db', Options(raw_mode=True))
    for b in rand_bytes:
        rdict[b] = b
    rdict.close()
    print('Benchmarking Rdict Iterator...')
    perf_iterator_single_thread(rand_bytes)
    perf_iterator_multi_thread(rand_bytes)
    print('Benchmarking Rdict Get...')
    perf_random_get_single_thread(rand_bytes)
    perf_random_get_multi_thread(rand_bytes)

    # Destroy the Rdict instance
    Rdict.destroy('test.db')
