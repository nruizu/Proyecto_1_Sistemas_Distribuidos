# master.py
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import FileResponse
from pydantic import BaseModel
import os, uuid, glob

app = FastAPI()

BASE = os.path.abspath("shared")
NUM_WORKERS = 0
JOBS = {}   # job_id -> dict
TASKS = []  # cola de tareas
WORKED = [] #lista de nodos que han trabajado (se usa para realizar el balance de cargas)
WORKERS = set()

class JobSpec(BaseModel):
    user_code_path: str
    dataset_path: str
    reducers: int = 2
    split_size_mb: int = 64

#funcion para asignar los pares key-values a los reducers
def part(key, R):
    return hash(str(key))%R

def merge_files(files, merge_path):
    with open(merge_path, "w", encoding="utf-8") as f1:
        for path in files:
            with open(path, "r", encoding="utf-8") as f2:
                f1.writelines(f2)

def sort_shuffle(job_id, job):

    intermediate_dir_map = job["intermediate_dir"]
    partitions_files_path = os.path.join(intermediate_dir_map, "*.txt")
    files = glob.glob(partitions_files_path)

    # Se crea el directorio intermedio para el sort and shuffle
    inter_reduce = os.path.join(BASE, "intermediate_reduce", f"job-{job_id}")
    os.makedirs(inter_reduce, exist_ok=True)

    # Archivo donde se juntan los mappers intermedios para ordenarlos
    merge_path = os.path.join(inter_reduce, "complete.txt")

    # Cantidad de reducers definidos
    R = job["reducers"]
    
    # Se crean los buckets (uno por cada reducer)
    for r in range(R):
        os.makedirs(os.path.join(inter_reduce, f"bucket-{r}"), exist_ok=True)

    merge_files(files, merge_path)

    with open(merge_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    lines.sort()
    with open(merge_path, "w", encoding="utf-8") as f:
        f.writelines(lines)

    i = 0
    with open(merge_path, "r", encoding="utf-8") as f:
        for lines in f:
            words = lines.split()
            r = part(words[0], R)
            with open(os.path.join(inter_reduce, f"bucket-{r}", f"reduce-{i:05d}.txt"), "a", encoding="utf-8") as f:
                f.write(lines)

    job["intermediate_dir"] = inter_reduce

def make_splits(dataset_path, split_dir, lines_per_split = 5):
    os.makedirs(split_dir, exist_ok=True)
    files = glob.glob(dataset_path)
    i = 0
    buff = []
    for path in files:
        with open(path, "r", errors="ignore") as f:
            for line in f:
                buff.append(line)
                if len(buff) >= lines_per_split:
                    with open(os.path.join(split_dir, f"split-{i:05d}"), "w", encoding="utf-8") as out:
                        out.writelines(buff)
                    buff = []
                    i += 1
    if buff:
        with open(os.path.join(split_dir, f"split-{i:05d}"), "w", encoding="utf-8") as out:
            out.writelines(buff)
        i += 1
    return i

@app.post("/jobs")
def create_job(spec: JobSpec):
    job_id = str(uuid.uuid4())[:8]
    split_dir = os.path.join(BASE, "splits", job_id)
    intermediate_dir = os.path.join(BASE, "intermediate_map", f"job-{job_id}")
    output_dir = os.path.join(BASE, "output", f"job-{job_id}")
    os.makedirs(intermediate_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    # Partir dataset (para el demo: por líneas)
    M = make_splits(spec.dataset_path, split_dir, lines_per_split=20)

    # Crear M tareas MAP
    for i in range(M):
        TASKS.append({
            "id": f"map-{job_id}-{i}",
            "type": "map",
            "state": "idle",
            "job_id": job_id,
            "split_path": os.path.join(split_dir, f"split-{i:05d}"),
            "reducers": spec.reducers,
            "user_code_path": spec.user_code_path,
            "intermediate_dir": intermediate_dir
        })

    JOBS[job_id] = {
        "status": "MAP",
        "reducers": spec.reducers,
        "intermediate_dir": intermediate_dir,
        "user_code_path": spec.user_code_path,
        "output_dir": output_dir,
        "split_dir": split_dir
    }
    return {"job_id": job_id, "maps": M, "reducers": spec.reducers}

@app.post("/jobs/upload")
def upload_job(
    code: UploadFile = File(...),
    dataset_path: str = Form(...),
    reducers: int = Form(2),
    split_size_mb: int = Form(64),
):
    job_id = str(uuid.uuid4())[:8]
    job_dir = os.path.join(BASE, "jobs", f"job-{job_id}")
    os.makedirs(job_dir, exist_ok=True)
    code_path = os.path.join(job_dir, "job.py")
    with open(code_path, "wb") as out:
        out.write(code.file.read())

    # Reutilizamos create_job logic:
    return create_job(JobSpec(
        user_code_path=code_path,
        dataset_path=dataset_path,
        reducers=reducers,
        split_size_mb=split_size_mb
    ))

@app.post("/workers/register")
def register(body: dict):
    global NUM_WORKERS
    global WORKERS
    wid = body.get("worker_id")
    if wid and wid not in WORKERS:
        WORKERS.add(wid)
        NUM_WORKERS = len(WORKERS)
    return {"ok": True, "workers": list(WORKERS)}

@app.post("/tasks/next")
def next_task(worker: dict):
    global NUM_WORKERS, WORKED
    if NUM_WORKERS == len(WORKED):
        WORKED = []
    if worker.get("worker_id", "w?") in WORKED:
        return {"task": None}
    # Primero intenta dar un MAP
    for t in TASKS:
        if t["state"] == "idle" and t["type"] == "map":
            t["state"] = "in-progress"
            t["worker"] = worker.get("worker_id", "w?")
            WORKED.append(t["worker"])
            return t

    # Si no quedan MAP pendientes, y aún no generamos reduce se generan solo 1 vez
    # Detecta si hay algún job en estado MAP con todos los MAP "done"
    remaining = None
    for job_id, job in JOBS.items():
        if job["status"] == "MAP":
            for task in TASKS:
                if task["type"]=="map" and task["job_id"]==job_id and task["state"]!="done":
                    remaining = task
            if not remaining:
                # Transición a REDUCE
                job["status"] = "REDUCE"
                # Se ordenan y se reparten los resultados de los mappers
                sort_shuffle(job_id, job)
                # Numero de reducers
                R = job["reducers"]
                for r in range(R):
                    TASKS.append({
                        "id": f"reduce-{job_id}-{r}",
                        "type": "reduce",
                        "state": "idle",
                        "job_id": job_id,
                        "bucket": r,
                        "intermediate_dir": job["intermediate_dir"],
                        "output_dir": job["output_dir"],
                        "user_code_path": job["user_code_path"]
                    })
            remaining = None

    # Entregar REDUCE si hay
    for t in TASKS:
        if t["state"] == "idle" and t["type"] == "reduce":
            t["state"] = "in-progress"
            t["worker"] = worker.get("worker_id", "w?")
            WORKED.append(t["worker"])
            return t
    
    

    return {"task": None}

@app.post("/tasks/{task_id}/result")
def task_result(task_id: str, body: dict):
    ok = bool(body.get("ok", True))
    job_id = None
    task_type = None
    remaining = None
    # Marca la tarea como done
    for t in TASKS:
        if t["id"] == task_id:
            t["state"] = "done" if ok else "idle"
            t["meta"] = body
            job_id = t["job_id"]
            task_type = t["type"]
            break

    
    if ok and task_type == "reduce" and job_id:
        for t in TASKS:
            if t["job_id"] == job_id and t["type"] == "reduce" and t["state"] != "done":
                remaining = t
        if not remaining:
            output_dir = os.path.join(BASE, "output", f"job-{job_id}")
            dir_b = os.path.join(output_dir, "*.txt")
            files = files = glob.glob(dir_b)
            output_file = os.path.join(output_dir, "Final.txt")
            merge_files(files, output_file)
            JOBS[job_id]["status"] = "DONE"
            JOBS[job_id]["final_output"] = output_file

    return {"ok": True}

@app.get("/jobs/{job_id}")
def job_status(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        return {"error": "unknown job"}
    for t in TASKS:
        if t["job_id"]==job_id and t["type"]=="map" and t["state"]=="done":
            done_maps = t
        elif t["job_id"]==job_id and t["type"]=="reduce" and t["state"]=="done":
            done_reduces = t            
    return {
        "status": job["status"],
        "maps_done": len(done_maps),
        "reduces_done": len(done_reduces),
        "output_dir": job["output_dir"]
    }

@app.get("/jobs/{job_id}/result")
def get_result(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        return {"error": "unknown job"}

    final_path = os.path.join(job["output_dir"], "Final.txt")
    if not os.path.exists(final_path):
        return {"error": "Final.txt not found"}

    return FileResponse(final_path, media_type="text/plain", filename="Final.txt")

@app.get("/_debug/tasks")
def debug_tasks():
    return TASKS

@app.get("/_debug/jobs")
def debug_jobs():
    return JOBS
