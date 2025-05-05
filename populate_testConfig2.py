config_lines = [
    "f 0",
    "replica 10.10.1.2:8080",
    "replica 10.10.1.3:8081",
    "replica 10.10.1.4:8082"
]

with open("testConfig2.txt", "w") as f:
    for line in config_lines:
        f.write(line + "\n")

print("testConfig2.txt has been written.")
