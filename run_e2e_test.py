#!/usr/bin/python3
import os
import subprocess
import time
import netifaces
import re
import random
import signal
import sys
import traceback
import tempfile
import select


server_process = None
client_process = None
mem_file_path = None

STATUS_NO_SUCH_KEY = 262
STATUS_OK = 0
STATUS_PERMISSION_DENIED = 269


# iterate /sys/class/infiniband, find any usable RDMA device, and return IPv4 or IPv6 address
def find_rdma_dev():
    ibclass = "/sys/class/infiniband/"
    try:
        for dev in os.listdir(ibclass):
            netdev = ibclass + dev + "/ports/1/gid_attrs/ndevs/0"
            with open(netdev) as fp:
                iface = fp.readline().strip("\n")
                try:
                    addrs = netifaces.ifaddresses(iface)
                except ValueError:
                    continue
                if netifaces.AF_INET in addrs:
                    ipv4_addr = addrs[netifaces.AF_INET][0]["addr"]
                    print(
                        f"---- E2E TEST: prepare {dev} <{ipv4_addr}> [OK] ----"
                    )
                    return ipv4_addr

                if netifaces.AF_INET6 in addrs:
                    ipv6_addr = addrs[netifaces.AF_INET6][0]["addr"]
                    print(
                        f"---- E2E TEST: prepare {dev} <{ipv6_addr}> [OK] ----"
                    )
                    return ipv6_addr
    except os.error:
        return None

    return None


def parse_status(stdout: str):
    """Parse all status(...) and return the last one to handle multi-stage outputs (e.g., acquire_get with a final RELEASE)."""
    matches = re.findall(r'status\((\d+)\)', stdout)
    if matches:
        return int(matches[-1])
    return None

# Simplified: removed unused parse_token


def check_status(stdout: str, expected_status: int) -> bool:
    status = parse_status(stdout)
    if status is None or status != expected_status:
        return False

    return True


def check_value(stdout: str, expected_value: str) -> bool:
    """Match value lines from both GET and ACQUIRE GET outputs."""
    m = re.search(r'.*value\[\d+\]=(.*)', stdout)
    if not m:
        return False
    value = m.group(1).strip()
    return value == expected_value

def parse_named_status(stdout: str, name: str) -> int:
    m = re.findall(rf'{name}[^\n]*status\((\d+)\)', stdout)
    if not m:
        return None
    return int(m[-1])

def check_named_status(stdout: str, name: str, expected_status: int) -> bool:
    s = parse_named_status(stdout, name)
    return s is not None and s == expected_status

def run_client_commands(ip, port, commands) -> str:
    """Execute multiple commands within the same client connection and return aggregated output."""
    create_client(ip, port)
    try:
        input_data = "\n".join(list(commands) + ["exit"]) + "\n"
        stdout = client_process.communicate(input_data)[0]
        return stdout
    finally:
        destroy_client()


def signal_handler(signum, frame):
    destroy_client()
    destroy_server()
    destroy_memfile()
    sys.exit(-1)


def create_memfile():
    global mem_file_path
    mem_file_name = f"memfile_{int(time.time())}_{random.randint(0,99999)}"
    def _tmpfs_path(name: str) -> str:
        uid = os.getuid()
        candidates = []
        try:
            with open("/proc/mounts") as f:
                mounts = [line.split() for line in f]
            tmpfs_mounts = {m[1] for m in mounts if len(m) >= 3 and m[2] == "tmpfs"}
            user_run = f"/run/user/{uid}"
            if user_run in tmpfs_mounts and os.path.isdir(user_run) and os.access(user_run, os.W_OK):
                candidates.append(user_run)
            if "/dev/shm" in tmpfs_mounts and os.access("/dev/shm", os.W_OK):
                candidates.append("/dev/shm")
            if "/run" in tmpfs_mounts and os.access("/run", os.W_OK):
                candidates.append("/run")
            for m in tmpfs_mounts:
                if os.access(m, os.W_OK):
                    candidates.append(m)
        except Exception:
            pass
        for base in candidates:
            return os.path.join(base, name)
        for base in (f"/run/user/{uid}", "/dev/shm", "/run"):
            if os.path.isdir(base) and os.access(base, os.W_OK):
                return os.path.join(base, name)
        return os.path.join(tempfile.gettempdir(), name)
    mem_file_path = _tmpfs_path(mem_file_name)

    subprocess.run([
        "./server/priskv-memfile", "-o", "create", "-f", mem_file_path,
        "--max-keys", "1024", "--max-key-length", "128", "--value-block-size",
        "4096", "--value-blocks", "4096"
    ],
                   stdout=subprocess.DEVNULL,
                   check=True)


def destroy_memfile():
    global mem_file_path
    if mem_file_path and os.path.exists(mem_file_path):
        os.remove(mem_file_path)


def create_client(ip, port):
    global client_process
    if client_process is not None:
        destroy_client()

    client_process = subprocess.Popen(
        ["./client/priskv-client", "-a", ip, "-p",
         str(port)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True)


def create_server(ip, port):
    global server_process
    if server_process is not None:
        destroy_server()

    server_process = subprocess.Popen([
        "./server/priskv-server", "-a", ip, "-A", "localhost", "-p",
        str(port), "-P",
        str(port + 1), "-f", mem_file_path, "--acl", "any"
    ],
                                      stdout=subprocess.PIPE,
                                      stderr=None)


def destroy_server():
    global server_process
    if server_process is not None:
        server_process.terminate()
        server_process.wait()
        server_process = None


def destroy_client():
    global client_process
    if client_process is not None:
        client_process.terminate()
        client_process.wait()
        client_process = None


def do_test(ip, port, cmd, status, value: str = None):
    create_client(ip, port)
    # Append newline and 'exit' to flush all output and exit cleanly
    stdout = client_process.communicate(cmd + "\nexit\n")[0]
    if not check_status(stdout, status) or (value is not None and
                                            not check_value(stdout, value)):
        print(stdout)
        cleanup()
        return 1

    destroy_client()
    return 0


def priskv_e2e_test():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    print("---- E2E TEST ----")

    port = random.randint(24300, 24500)

    print("---- E2E TEST (RDMA) ----")
    rdma_ip = find_rdma_dev()
    if rdma_ip is None:
        print("---- No RDMA IP, SKIP E2E TEST OVER RDMA ----")
    else:
        priskv_e2e_test_run(rdma_ip, port)

    ucx_wireup_ip = "0.0.0.0"
    os.environ["PRISKV_TRANSPORT"] = "ucx"

    print("---- E2E TEST (UCX TCP) ----")
    os.environ["UCX_TLS"] = "tcp"
    priskv_e2e_test_run(ucx_wireup_ip, port)

    print("---- E2E TEST (UCX SM) ----")
    os.environ["UCX_TLS"] = "sm"
    priskv_e2e_test_run(ucx_wireup_ip, port)

    print("---- E2E TEST (UCX ) ----")
    os.environ["PRISKV_USE_SHM"] = "y"
    priskv_e2e_test_run(ucx_wireup_ip, port)

def priskv_e2e_test_run(ip, port):
    create_memfile()
    print("---- E2E TEST: create memfile [OK] ----")
    try:
        create_server(ip, port)

        # wait for server ready
        time.sleep(10)
        key = 123
        value = 456

        # step 1, get key from empty KV
        ret = do_test(ip, port, f"get {key}", STATUS_NO_SUCH_KEY)
        if ret != 0:
            print("---- E2E TEST: get key from empty KV [FAILED] ----")
            return ret

        print("---- E2E TEST: get key from empty KV [OK] ----")

        # step 2, set key to empty KV
        ret = do_test(ip, port, f"set {key} {value}", STATUS_OK)
        if ret != 0:
            print("---- E2E TEST: set key to empty KV [FAILED] ----")
            return ret

        print("---- E2E TEST: set key to empty KV [OK] ----")

        ## step 3, verify key from filled KV
        ret = do_test(ip, port, f"get {key}", STATUS_OK, str(value))
        if ret != 0:
            print("---- E2E TEST: verify key from filled KV [FAILED] ----")
            return ret

        print("---- E2E TEST: verify key from filled KV [OK] ----")

        # step 4, delete key from filled KV
        ret = do_test(ip, port, f"delete {key}", STATUS_OK)
        if ret != 0:
            print("---- E2E TEST: delete key from filled KV [FAILED] ----")
            return ret

        print("---- E2E TEST: delete key from filled KV [OK] ----")

        # step 5, get key from empty KV
        ret = do_test(ip, port, f"get {key}", STATUS_NO_SUCH_KEY)
        if ret != 0:
            print("---- E2E TEST: get key from empty KV [FAILED] ----")
            return ret

        print("---- E2E TEST: get key from empty KV [OK] ----")

        # step 6, set key with timeout 5s
        ret = do_test(ip, port, f"set {key} {value} EX 5", STATUS_OK)
        if ret != 0:
            print("---- E2E TEST: set key with timeuot 5s [FAILED] ----")
            return ret

        print("---- E2E TEST: set key with timeuot 5s [OK] ----")

        # step 7, get key from kv before expired and compare value
        time.sleep(3)
        ret = do_test(ip, port, f"get {key}", STATUS_OK, str(value))
        if ret != 0:
            print(
                "---- E2E TEST: verify key from filled KV before expired [FAILED] ----"
            )
            return ret

        print(
            "---- E2E TEST: verify key from filled KV before expired [OK] ----"
        )

        # step 8, get key after expired
        time.sleep(5)
        ret = do_test(ip, port, f"get {key}", STATUS_NO_SUCH_KEY)
        if ret != 0:
            print("---- E2E TEST: get key after expired [FAILED]----")
            return ret

        print("---- E2E TEST: get key after expired [OK] ----")

        # step 9, set key to empty KV without timeout
        ret = do_test(ip, port, f"set {key} {value}", STATUS_OK)
        if ret != 0:
            print(
                "---- E2E TEST: set key to empty KV without timeout [FAILED]----"
            )
            return ret

        print("---- E2E TEST: set key to empty KV without timeout [OK] ----")

        # step 10, get keys from KV and compare values after a while
        time.sleep(7)
        ret = do_test(ip, port, f"get {key}", STATUS_OK, str(value))
        if ret != 0:
            print("---- E2E TEST: verify keys from filled KV [FAILED]----")
            return ret

        print("---- E2E TEST: verify keys from filled KV [OK] ----")

        # step 11, set expire time 5s
        ret = do_test(ip, port, f"expire {key} 5", STATUS_OK)
        if ret != 0:
            print("---- E2E TEST: set expire time 5s [FAILED] ----")
            return ret

        print("---- E2E TEST: set expire time 5s [OK] ----")

        # step 12, get keys from empty KV
        time.sleep(7)
        ret = do_test(ip, port, f"get {key}", STATUS_NO_SUCH_KEY)
        if ret != 0:
            print("---- E2E TEST: get keys from empty KV [FAILED] ----")
            return ret

        print("---- E2E TEST: get keys from empty KV [OK] ----")

        # ===== ZeroCopy end-to-end, gated by PRISKV_USE_SHM =====
        if os.environ.get("PRISKV_USE_SHM", 'n') == 'y':
            zkey = str(789)
            zval = str(42)
            # Composite commands: publish (ALLOC+SEAL) and read (ACQUIRE+RELEASE)
            ret = do_test(ip, port, f"alloc_set {zkey} {zval}", STATUS_OK)
            if ret != 0:
                print("---- E2E TEST: alloc_set failed [FAILED] ----")
                return ret
            ret = do_test(ip, port, f"acquire_get {zkey}", STATUS_OK, zval)
            if ret != 0:
                print("---- E2E TEST: acquire_get failed [FAILED] ----")
                return ret
            print("---- E2E TEST: ZeroCopy alloc_set + acquire_get [OK] ----")

            # Atomic commands: alloc/seal/acquire/release
            stdout = run_client_commands(ip, port, [
                f"alloc {zkey}_atom 16",
                "seal last",
                f"acquire {zkey}_atom",
                "release last",
            ])
            if not (check_named_status(stdout, "ALLOC", STATUS_OK) and
                    check_named_status(stdout, "SEAL", STATUS_OK) and
                    check_named_status(stdout, "ACQUIRE", STATUS_OK) and
                    check_named_status(stdout, "RELEASE", STATUS_OK)):
                print(stdout)
                print("---- E2E TEST: atomic alloc+seal+acquire+release [FAILED] ----")
                return 1
            print("---- E2E TEST: atomic alloc+seal+acquire+release [OK] ----")

            # Atomic: drop unpublished token and subsequent acquire should be NO_SUCH_KEY
            stdout = run_client_commands(ip, port, [
                f"alloc {zkey}_unpub 16",
                "drop last",
                f"acquire {zkey}_unpub",
            ])
            if not (check_named_status(stdout, "DROP", STATUS_OK) and
                    check_named_status(stdout, "ACQUIRE", STATUS_NO_SUCH_KEY)):
                print(stdout)
                print("---- E2E TEST: atomic drop(unpublished)+acquire [FAILED] ----")
                return 1
            print("---- E2E TEST: atomic drop(unpublished)+acquire [OK] ----")
        else:
            print("---- E2E TEST: skip ZeroCopy (PRISKV_USE_SHM!=y) ----")

        return 0
    except Exception as e:
        traceback.print_exc()
        return 1
    finally:
        cleanup()


def cleanup():
    destroy_server()
    destroy_client()
    destroy_memfile()

    print("---- E2E TEST DONE ----")


if __name__ == "__main__":
    exit_code = priskv_e2e_test()
    if exit_code != 0:
        exit(exit_code)
