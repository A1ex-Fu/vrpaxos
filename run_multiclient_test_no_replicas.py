import subprocess
import os
import re
import time
import statistics
import csv
import matplotlib
matplotlib.use('Agg')  
import matplotlib.pyplot as plt

# Reproduce figure8 chart from specpaxos paper


CLIENT_BIN = "./bench/client"
CONFIG_FILE = "testConfig2.txt"
PROTOCOL = "vr" 
OUTPUT_DIR = "outputs"
RESULT_DIR = "outputs"
CSV_EXPORT = True
TOTAL_REQUESTS = 10
MIN_REQUESTS_PER_CLIENT = 3
CLIENT_COUNTS = [2, 5, 10, 20, 50, 100, 150, 200, 250, 300]
CLIENT_COUNTS = [300]
REPLICA_COUNT = 3
WARMUP_TIME = 5 

for directory in [OUTPUT_DIR, RESULT_DIR]:
    if not os.path.exists(directory):
        os.makedirs(directory)

for f in os.listdir(OUTPUT_DIR):
    os.remove(os.path.join(OUTPUT_DIR, f))

THROUGHPUT_RE = re.compile(r"Completed\s+(\d+)\s+requests\s+in\s+([0-9.]+)\s+seconds")
MEDIAN_LAT_RE = re.compile(r"Median latency is\s+([0-9]+)\s+ns")





def run_clients(n_clients, protocol):
    print(f"\n=== Running {n_clients} clients for protocol '{protocol}' ===")
    procs = []
    requests_per_client = max(MIN_REQUESTS_PER_CLIENT, TOTAL_REQUESTS // n_clients)
    outputs = []

    for i in range(n_clients):
        print(f"\n[Launching client {i}]")
        p = subprocess.Popen([ 
            CLIENT_BIN,
            "-c", CONFIG_FILE,
            "-m", protocol,
            "-n", str(requests_per_client),
            "-t", "1"
        ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


        procs.append((i, p))

  

    for i, p in procs:
        print(f"\n[Client {i} Output]")
        stdout, _ = p.communicate()
        print(stdout)
        outputs.append((i, stdout))
        p.wait()
    throughputs = []
    medians = []

    for i, stdout in outputs:
        client_tput = None
        client_median = None

        for line in stdout.splitlines():
            t_match = THROUGHPUT_RE.search(line)
            if t_match:
                try:
                    completed = int(t_match.group(1))
                    seconds = float(t_match.group(2))
                    client_tput = completed / seconds
                except Exception as e:
                    print(f"[ParseError] Throughput: {e}")

            lat_match = MEDIAN_LAT_RE.search(line)
            if lat_match:
                try:
                    latency_ns = int(lat_match.group(1))
                    client_median = latency_ns / 1000  #convert to µs
                except Exception as e:
                    print(f"[ParseError] Latency: {e}")


        if client_tput is not None and client_median is not None:
            throughputs.append(client_tput)
            medians.append(client_median)
            print(f"\n→ Client {i}: Throughput = {client_tput:.2f} ops/sec, Median Latency = {client_median:.1f} µs")
        else:
            print(f"\n→ Client {i}: Failed to collect complete data.")

    if throughputs and medians:
        total_tput = sum(throughputs)
        median_latency = statistics.median(medians)
        print(f"\n→ Average Throughput: {total_tput:.2f} ops/sec, Median of Medians Latency: {median_latency:.1f} µs")
        return total_tput, median_latency
    else:
        print("\n→ Failed to collect data from any client.")
        return None, None

def plot_results(results, protocol):
    results = [r for r in results if r[1] is not None]
    throughputs = [r[1] for r in results]
    latencies = [r[2] for r in results]

    plt.figure(figsize=(8, 6))
    plt.plot(throughputs, latencies, marker='o', label=protocol)

    for i in range(len(CLIENT_COUNTS)):
        label = f"{CLIENT_COUNTS[i]}"
        plt.annotate(label,
                     (throughputs[i], latencies[i]),
                     textcoords="offset points",
                     xytext=(5, 5),
                     ha='left', fontsize=8, color='gray')

    plt.xlabel("Throughput (ops/sec)")
    plt.ylabel("Median Latency (µs)")
    plt.title(f"Median Latency vs Throughput — {protocol}")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()

    plot_path = os.path.join(RESULT_DIR, f"latency_vs_throughput_{protocol}.png")
    plt.savefig(plot_path)
    print(f"Plot saved as {plot_path}")

def export_csv(results, protocol):
    csv_path = os.path.join(RESULT_DIR, f"results_{protocol}.csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Clients", "Throughput_ops", "Median_Latency_us"])
        for row in results:
            writer.writerow(row)
    print(f"Exported CSV: {csv_path}")

def load_existing_results(csv_path):
    existing = {}
    if os.path.exists(csv_path):
        with open(csv_path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    n = int(row["Clients"])
                    tput = float(row["Throughput_ops"])
                    lat = float(row["Median_Latency_us"])
                    existing[n] = (tput, lat)
                except Exception as e:
                    print(f"[CSV ParseError] {e}")
    return existing

def append_result(csv_path, clients, tput, median):
    new_file = not os.path.exists(csv_path)
    with open(csv_path, "a", newline="") as f:
        writer = csv.writer(f)
        if new_file:
            writer.writerow(["Clients", "Throughput_ops", "Median_Latency_us"])
        writer.writerow([clients, tput, median])

def main():
    all_results = []
    csv_path = os.path.join(RESULT_DIR, f"results_{PROTOCOL}.csv")
    existing_results = load_existing_results(csv_path)

    for n_clients in CLIENT_COUNTS:
        if n_clients in existing_results:
            tput, median = existing_results[n_clients]
            print(f"→ Skipping {n_clients} clients (cached): Throughput = {tput:.2f}, Latency = {median:.1f}")
            all_results.append((n_clients, tput, median))
            continue

        time.sleep(WARMUP_TIME)

        tput, median = run_clients(n_clients, PROTOCOL)

        if tput is not None and median is not None:
            append_result(csv_path, n_clients, tput, median)
            all_results.append((n_clients, tput, median))
    final_results = sorted(load_existing_results(csv_path).items())
    plot_results([(n, t, l) for n, (t, l) in final_results], PROTOCOL)


if __name__ == "__main__":
    main()
