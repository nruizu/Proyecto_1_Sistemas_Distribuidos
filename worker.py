# worker.py
import os, sys, time, glob, requests
from collections import defaultdict
from itertools import groupby
from dotenv import load_dotenv
import hashlib

load_dotenv("/home/ubuntu/.env")

MASTER = os.environ["MASTER_URL"]
WORKER_ID = os.environ["WORKER_ID"]
MAP_COUNT = 0

print(f"[BOOT] MASTER={MASTER} WORKER_ID={WORKER_ID}")
if not MASTER or not WORKER_ID:
    sys.exit("Error: faltan variables MASTER_URL o WORKER_ID en el entorno (.env)")

def load_user(user_code_path):
    import importlib.util
    spec = importlib.util.spec_from_file_location("jobmod", user_code_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    map_fn = getattr(mod, "map")
    reduce_fn = getattr(mod, "reduce")
    return map_fn, reduce_fn

def run_map(task):
    global MAP_COUNT
    MAP_COUNT += 1
    user_code = task["user_code_path"]
    map_fn = load_user(user_code)[0]
    R = task["reducers"]
    inter_base = task["intermediate_dir"]
    os.makedirs(inter_base, exist_ok=True)
    # devuelve una lista de diccionarios predeterminados
    mapped = defaultdict(int)

    with open(task["split_path"], "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            for k, v in map_fn(line):
                mapped[k] += v

    outp = os.path.join(inter_base, f"mapper-{WORKER_ID}{MAP_COUNT}.txt")
    with open(outp, "w", encoding="utf-8") as out:
        for k, c in mapped.items():
            print(f"{k} {c}")
            out.write(f"{k} {c}\n")

def run_reduce(task):
    inter = task["intermediate_dir"]
    outdir = task["output_dir"]
    r = task["bucket"]
    user_code = task["user_code_path"]
    reduce_fn = load_user(user_code)[1]
    outp = os.path.join(outdir, f"part-{r:05d}.txt")

    lines = []
    for p in glob.glob(os.path.join(inter, f"bucket-{r}", "*.txt")):
        with open(p, "r", encoding="utf-8") as f:
            for line in f:
                k, v = line.strip().split()
                lines.append((k, int(v)))

    with open(outp, "a", encoding="utf-8") as out:
        for key, group in groupby(lines, key=lambda kv: kv[0]):
            values_list = [v for _, v in group]
            total = reduce_fn(key, values_list)
            out.write(f"{key} {total}\n")

def main():
    requests.post(f"{MASTER}/workers/register", json={"worker_id": WORKER_ID})
    while True:
        t = requests.post(f"{MASTER}/tasks/next", json={"worker_id": WORKER_ID}).json()
        if not t or not t.get("id"):
            time.sleep(0.5)
            continue
        try:
            if t["type"] == "map":
                run_map(t)
            else:
                run_reduce(t)
            requests.post(f"{MASTER}/tasks/{t['id']}/result", json={"ok": True})
        except Exception as e:
            requests.post(f"{MASTER}/tasks/{t['id']}/result", json={"ok": False, "error": str(e)})

if __name__ == "__main__":
    main()
