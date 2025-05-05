import subprocess
import os
import time

# Run basic 2R+1W and 1 client case with the following client command:
CLIENT_CMD = ["./bench/client", "-c", "./testConfig2.txt", "-m", "vr", "-n", "1000", "-t", "1", "-w", "5", "-l", "latency.txt"]


REPLICA_COUNT = 3  # 2 replicas + 1 witness
OUTPUT_DIR = "client_logs"
CONFIG_FILE = "testConfig2.txt"
PROTOCOL = "vr"

os.makedirs(OUTPUT_DIR, exist_ok=True)

def start_replicas():
    replicas = []
    def launch_replica(index, is_witness=False):
        log_path = os.path.join(OUTPUT_DIR, f"replica_{index}{'_witness' if is_witness else ''}.err")
        log_file = open(log_path, "w")
        args = ["./bench/replica", "-c", CONFIG_FILE, "-i", str(index), "-m", PROTOCOL]
        if is_witness:
            args.append("-w")
        proc = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=log_file, text=True)
        replicas.append((proc, log_file))

    launch_replica(0)
    launch_replica(1, is_witness=True)
    launch_replica(2)
    time.sleep(2)
    return replicas

def kill_replicas(replicas):
    for proc, _ in replicas:
        proc.terminate()
    for proc, log_file in replicas:
        proc.wait()
        log_file.close()

def main():
    replicas = start_replicas()
    print(f"\n[Running client: {' '.join(CLIENT_CMD)}]\n")
    subprocess.run(CLIENT_CMD)

    kill_replicas(replicas)

if __name__ == "__main__":
    main()
