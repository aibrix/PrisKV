#!/usr/bin/python3
import os
import subprocess
import threading
import time
import argparse


def run_test(prog, result_list, index):
    start = time.time()
    p = subprocess.Popen(prog,
                         shell=True,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    if p.wait():
        print("PrisKV Unit test %s [FAILED]" % prog, end='')
        result_list[index] = 1
    else:
        print("PrisKV Unit test %s [OK]" % prog, end='')
        result_list[index] = 0
    end = time.time()
    elapsed_time = (end - start) * 1000 * 1000
    print(f" ... [{elapsed_time:.0f} us]")
    if len(stdout.decode()) > 0:
        print(f"    {stdout.decode()}", end='')
    if len(stderr.decode()) > 0:
        print(f"    {stderr.decode()}", end='')


def priskv_unit_test(parallel: bool = True):
    progs = [
        # "lib/test/test-event", "lib/test/test-threads", "lib/test/test-codec",
        "./server/test/test-slab-mt", "./server/test/test-buddy",
        "./server/test/test-buddy-mt", "./server/test/test-kv",
        "./server/test/test-kv-mt", "./server/test/test-memory --no-tmpfs",
        "./server/test/test-slab"
    ]

    print("---- PrisKV UNIT TEST ----")
    result_list = [0] * len(progs)

    if parallel:
        threads = []
        for i, prog in enumerate(progs):
            thread = threading.Thread(target=run_test, args=(prog, result_list, i))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()
    else:
        for i, prog in enumerate(progs):
            run_test(prog, result_list, i)

    print("---- PrisKV UNIT TEST DONE ----")
    if any(result_list):
        return 1
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run PrisKV unit tests")
    parser.add_argument(
        "--no-parallel",
        dest="no_parallel",
        action="store_true",
        help="Disable parallel running and run tests sequentially"
    )
    args = parser.parse_args()

    exit_code = priskv_unit_test(parallel=not args.no_parallel)
    if exit_code != 0:
        exit(exit_code)
