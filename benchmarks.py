import os
from subprocess import run
from pathlib import Path
import json
import re
import pandas as pd
import matplotlib.pyplot as plt

BENCH_RESULT_FOLDER = Path("./bench_result")
BENCH_PLOT_FOLDER = Path("./bench_plot")
TOTAL_ROUNDS = 5
RESULT_FILE_NAME = re.compile("bench_(.*?)_n(\\d+)_k(\\d+)_v(\\d+)_r(\\d+)\\.json")
N_K_V = [(10000, 16, 100), (1000, 16, 100000)]
TEST_NAME_DICT = {'test_fill_raw_sequential': 'insert_sequential',
                  'test_fill_raw_batch_sequential': 'insert_sequential',
                  'test_read_hot_raw': 'random_read',
                  'test_delete_sequential_raw': 'delete_sequential',
                  'test_read_sequential_raw': 'read_sequential',
                  'test_get_raw_batch_sequential': 'read_sequential'}


def cmd(dbname, num, k_size, v_size, b_size, rounds):
    command = f"pytest " \
              f"-s --dbname {dbname} " \
              f"--num {num} " \
              f"--k_size {k_size} " \
              f"--v_size {v_size} " \
              f"--batch_size {b_size} " \
              f"--percent 1.0 " \
              f"benchmark.py " \
              f"--benchmark-json " \
              f"./{BENCH_RESULT_FOLDER}/" \
              f"bench_{dbname}_n{num}_k{k_size}_v{v_size}_r{rounds}.json".split()
    print(f"ROUND {rounds}:", " ".join(command))
    return command


def from_file_name(file_name):
    db, num_keys, k_size, v_size, rounds = RESULT_FILE_NAME.findall(file_name)[0]
    return {
        "db_name": db,
        "num_keys": int(num_keys),
        "key_size": int(k_size),
        "value_size": int(v_size),
        "rounds": int(rounds),
    }


def load_bench_result():
    results_files = [f for f in os.listdir(BENCH_RESULT_FOLDER)
                     if f.startswith("bench") and f.endswith(".json")]
    bench_results = []
    for r in results_files:
        with open(BENCH_RESULT_FOLDER / r, "r") as f:
            bench_data = json.load(f)
            bench_meta = from_file_name(r)
            for group in bench_data["benchmarks"]:
                new_row = bench_meta.copy()
                new_row.update({"test_name": group["name"],
                                "mean": group["stats"]["mean"],
                                "ops": group["stats"]["ops"], })
                bench_results.append(new_row)
                data_frame = pd.DataFrame(bench_results)
    return data_frame


def plot_single_test(test_name: str, num_keys: int,
                     key_size: int, value_size: int, r_df: pd.DataFrame):
    title = f'{TEST_NAME_DICT[test_name]}' \
            f'(num_keys={num_keys}, ' \
            f'ksize={key_size}, ' \
            f'vsize={value_size})'
    df_slice = r_df[(r_df['test_name'] == test_name)
                    & (r_df['num_keys'] == num_keys)
                    & (r_df['key_size'] == key_size)
                    & (r_df['value_size'] == value_size)]
    df_slice.set_index('db_name', inplace=True)
    ops = 1 / df_slice['mean'] * df_slice['num_keys']
    ax = ops.plot.bar(title=title, ylabel='ops')
    out_title = title.replace(',', '-') + '.png'
    plt.xticks(rotation=-20)
    fig = ax.get_figure()
    fig.savefig(BENCH_PLOT_FOLDER / out_title, pad_inches=0)
    plt.show()


def plot_benchmarks(df: pd.DataFrame, num_keys_ksize_vsize_list: list):
    result_df = df.groupby(['db_name', 'num_keys',
                            'key_size', 'value_size',
                            'test_name'])['mean'].mean().reset_index()
    test_names = [n for n in df['test_name'].unique()
                  if "raw" in n and "batch" not in n]
    for n_k_v in num_keys_ksize_vsize_list:
        num_keys, ksize, vsize = n_k_v
        for test in test_names:
            plot_single_test(test, num_keys, ksize, vsize, result_df)


if __name__ == '__main__':
    os.chdir("./benchmark")
    if not BENCH_RESULT_FOLDER.exists():
        os.mkdir(BENCH_RESULT_FOLDER)
    if not BENCH_PLOT_FOLDER.exists():
        os.mkdir(BENCH_PLOT_FOLDER)
    for r in range(TOTAL_ROUNDS):
        run(cmd("rocks_db_raw", 10000, 16, 100, 1000, r))
        run(cmd("py_vidar_db", 10000, 16, 100, 1000, r))
        run(cmd("semi_dbm", 10000, 16, 100, 1000, r))
        run(cmd("rocks_db_raw", 1000, 16, 100000, 100, r))
        run(cmd("py_vidar_db", 1000, 16, 100000, 100, r))
        run(cmd("semi_dbm", 1000, 16, 100000, 100, r))

    plot_benchmarks(load_bench_result(), N_K_V)
