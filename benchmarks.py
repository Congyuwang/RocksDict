import os
from subprocess import run
from pathlib import Path


BENCH_RESULT_FOLDER = Path("./bench_result")


def cmd(dbname, num, k_size, v_size, b_size):
    return f"pytest " \
           f"-s --dbname {dbname} " \
           f"--num {num} " \
           f"--k_size {k_size} " \
           f"--v_size {v_size} " \
           f"--batch_size {b_size} " \
           f"--percent 1.0 " \
           f"benchmark.py " \
           f"--benchmark-json " \
           f"./{BENCH_RESULT_FOLDER}/" \
           f"bench_{dbname}_n{num}_k{k_size}_v{v_size}.json".split()


if __name__ == '__main__':
    os.chdir("./benchmark")
    if not BENCH_RESULT_FOLDER.exists():
        os.mkdir(BENCH_RESULT_FOLDER)
    run(cmd("rocks_db", 10000, 16, 100, 1000))
    run(cmd("rocks_db_raw", 10000, 16, 100, 1000))
    run(cmd("sqlite_db", 10000, 16, 100, 1000))
    run(cmd("sqlite_db_raw", 10000, 16, 100, 1000))
    run(cmd("shelve_db", 10000, 16, 100, 1000))
    run(cmd("dbm", 10000, 16, 100, 1000))
    run(cmd("py_vidar_db", 10000, 16, 100, 1000))
    run(cmd("semi_dbm", 10000, 16, 100, 1000))
    run(cmd("cannon_db", 10000, 16, 100, 1000))
    run(cmd("rocks_db", 1000, 16, 100000, 100))
    run(cmd("rocks_db_raw", 1000, 16, 100000, 100))
    run(cmd("sqlite_db", 1000, 16, 100000, 100))
    run(cmd("sqlite_db_raw", 1000, 16, 100000, 100))
    run(cmd("shelve_db", 1000, 16, 100000, 100))
    run(cmd("dbm", 1000, 16, 100000, 100))
    run(cmd("py_vidar_db", 1000, 16, 100000, 100))
    run(cmd("semi_dbm", 1000, 16, 100000, 100))
    run(cmd("cannon_db", 1000, 16, 100000, 100))
