from rocksdict import Rdict, Options, WriteBatch, WriteOptions
from random import randbytes
from threading import Thread
from typing import List
import time


def gen_rand_bytes() -> List[bytes]:
    return [randbytes(128) for _ in range(4 * 1024 * 1024)]


def perf_put_single_thread(rand_bytes: List[bytes]):
    rdict = Rdict("test.db", Options(raw_mode=True))

    batch = WriteBatch(raw_mode=True)
    for k in rand_bytes:
        batch.put(k, k)

    start = time.perf_counter()
    # Make the write sync so this doesn't just benchmark system cache state.
    write_opt = WriteOptions()
    write_opt.sync = True
    rdict.write(batch, write_opt=write_opt)
    end = time.perf_counter()
    print(
        "Put performance: {} items in {} seconds".format(len(rand_bytes), end - start)
    )
    count = 0
    for k, v in rdict.items():
        assert k == v
        count += 1
    assert count == len(rand_bytes), f"{count=} != {len(rand_bytes)}"
    rdict.close()
    Rdict.destroy("test.db")


def perf_put_multi_thread(rand_bytes: List[bytes], num_threads: int):
    rdict = Rdict("test.db", Options(raw_mode=True))

    def perf_put(batch: WriteBatch):
        # Make the write sync so this doesn't just benchmark system cache state.
        write_opt = WriteOptions()
        write_opt.sync = True
        rdict.write(batch, write_opt=write_opt)

    threads = []
    each_len = len(rand_bytes) // num_threads
    batches = []
    for i in range(num_threads):
        batch = WriteBatch(raw_mode=True)
        for val in rand_bytes[i * each_len : (i + 1) * each_len]:
            batch.put(val, val)
        batches.append(batch)

    start = time.perf_counter()
    for batch in batches:
        t = Thread(target=perf_put, args=(batch,))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    end = time.perf_counter()
    print(
        "Put performance multi-thread: {} items in {} seconds".format(
            len(rand_bytes), end - start
        )
    )

    count = 0
    for k, v in rdict.items():
        assert k == v
        count += 1
    assert count == len(rand_bytes), f"{count=} != {len(rand_bytes)}"
    rdict.close()
    Rdict.destroy("test.db")


def perf_iterator_single_thread(rand_bytes: List[bytes]):
    rdict = Rdict("test.db", Options(raw_mode=True))
    start = time.perf_counter()
    count = 0
    for k, v in rdict.items():
        assert k == v
        count += 1
    end = time.perf_counter()
    assert count == len(rand_bytes)

    num_items = count
    secs = end - start
    item_per_sec = num_items / secs
    print(
        f"Iterator performance: {num_items} items in {secs} seconds ({item_per_sec} it/s)"
    )
    rdict.close()


def perf_iterator_multi_thread(rand_bytes: List[bytes], num_threads: int):
    rdict = Rdict("test.db", Options(raw_mode=True))
    start = time.perf_counter()

    def perf_iter():
        count = 0
        for k, v in rdict.items():
            assert k == v
            count += 1
        assert count == len(rand_bytes)

    threads = []
    for _ in range(num_threads):
        t = Thread(target=perf_iter)
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    end = time.perf_counter()

    num_items = num_threads * len(rand_bytes)
    secs = end - start
    item_per_sec = num_items / secs
    print(
        f"Iterator performance multi-thread: {num_items} items in {secs} seconds ({item_per_sec} it/s)"
    )
    rdict.close()


def perf_iterator_chunk_single_thread(
    rand_bytes: List[bytes], chunk_size: int, num_iterations: int
):
    rdict = Rdict("test.db", Options(raw_mode=True))

    start = time.perf_counter()
    for _ in range(num_iterations):
        items = []
        for batch in rdict.chunked_items(chunk_size=chunk_size):
            items.extend(batch)
    end = time.perf_counter()

    num_items = len(items) * num_iterations
    secs = end - start
    item_per_sec = num_items / secs
    print(
        f"Batched iterator performance: {num_items} items in {secs} seconds ({item_per_sec} it/s)"
    )
    rdict.close()


def perf_iterator_chunk_multi_thread(
    rand_bytes: List[bytes], num_threads: int, batch_size: int
):
    rdict = Rdict("test.db", Options(raw_mode=True))
    start = time.perf_counter()

    def perf_iter():
        items = []
        for batch in rdict.chunked_items(batch_size):
            items.extend(batch)

        assert len(items) == len(rand_bytes)

    threads = []
    for _ in range(num_threads):
        t = Thread(target=perf_iter)
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    end = time.perf_counter()

    num_items = num_threads * len(rand_bytes)
    secs = end - start
    item_per_sec = num_items / secs
    print(
        f"Batched iterator performance multi-thread: {num_items} items in {secs} seconds ({item_per_sec} it/s)"
    )
    rdict.close()


def perf_random_get_single_thread(rand_bytes: List[bytes]):
    rdict = Rdict("test.db", Options(raw_mode=True))
    start = time.perf_counter()
    vals = rdict.get(rand_bytes)
    for key, val in zip(rand_bytes, vals):
        assert key == val
    end = time.perf_counter()
    print(
        "Get performance: {} items in {} seconds".format(len(rand_bytes), end - start)
    )
    rdict.close()


def perf_random_get_multi_thread(rand_bytes: List[bytes], num_threads: int):
    rdict = Rdict("test.db", Options(raw_mode=True))
    start = time.perf_counter()

    def perf_get(keys: List[bytes]):
        vals = rdict.get(keys)
        for key, val in zip(keys, vals):
            assert key == val

    threads = []
    each_len = len(rand_bytes) // num_threads
    for i in range(num_threads):
        t = Thread(
            target=perf_get, args=(rand_bytes[i * each_len : (i + 1) * each_len],)
        )
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    end = time.perf_counter()
    print(
        "Get performance multi-thread: {} items in {} seconds".format(
            len(rand_bytes), end - start
        )
    )
    rdict.close()


if __name__ == "__main__":
    print("Gen rand bytes...")
    rand_bytes = gen_rand_bytes()

    NUM_THREADS = 4
    ITER_CHUNK_SIZE = 10_000

    print()
    print("Benchmarking Rdict Put...")
    # perf write
    perf_put_single_thread(rand_bytes)
    perf_put_multi_thread(rand_bytes, num_threads=NUM_THREADS)

    # Create a new Rdict instance
    rdict = Rdict("test.db", Options(raw_mode=True))
    for b in rand_bytes:
        rdict[b] = b
    rdict.close()

    print()
    print("Benchmarking Rdict Iterator...")
    perf_iterator_single_thread(rand_bytes)
    perf_iterator_multi_thread(rand_bytes, num_threads=NUM_THREADS)

    print()
    print("Benchmarking Rdict Batch Iterator...")
    perf_iterator_chunk_single_thread(
        rand_bytes, chunk_size=ITER_CHUNK_SIZE, num_iterations=NUM_THREADS
    )
    perf_iterator_chunk_multi_thread(
        rand_bytes, num_threads=NUM_THREADS, batch_size=ITER_CHUNK_SIZE
    )

    print()
    print("Benchmarking Rdict Get...")
    perf_random_get_single_thread(rand_bytes)
    perf_random_get_multi_thread(rand_bytes, num_threads=NUM_THREADS)

    # Destroy the Rdict instance
    Rdict.destroy("test.db")
