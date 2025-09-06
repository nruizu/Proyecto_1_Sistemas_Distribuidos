# master.py
from fastapi import FastAPI, UploadFile, File, Form
from pydantic import BaseModel
import os, uuid

app = FastAPI()

BASE = os.path.abspath("shared")
JOBS = {}   # job_id -> dict
TASKS = []  # cola de tareas

class JobSpec(BaseModel):
    user_code_path: str
    dataset_path: str
    reducers: int = 2
    split_size_mb: int = 64

def make_splits(dataset_path, split_dir, lines_per_split = 5):
    os.makedirs(split_dir, exist_ok=True)
    with open(dataset_path, "r", encoding="utf-8") as f:
        i = 0
        buff = []
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
    intermediate_dir = os.path.join(BASE, "intermediate", f"job-{job_id}")
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
    return {"ok": True}

@app.post("/tasks/next")
def next_task(worker: dict):
    # Primero intenta dar un MAP
    for t in TASKS:
        if t["state"] == "idle" and t["type"] == "map":
            t["state"] = "in-progress"
            t["worker"] = worker.get("worker_id", "w?")
            return t

    # Si no quedan MAP pendientes, y aún no generamos REDUCE → generarlos 1 sola vez
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

    # Entregar REDUCE si hay
    for t in TASKS:
        if t["state"] == "idle" and t["type"] == "reduce":
            t["state"] = "in-progress"
            t["worker"] = worker.get("worker_id", "w?")
            return t

    return {"task": None}

@app.post("/tasks/{task_id}/result")
def task_result(task_id: str, body: dict):
    # Marca la tarea como done
    for t in TASKS:
        if t["id"] == task_id:
            t["state"] = "done"
            t["meta"] = body
            break

    # Si no quedan tareas del job → marcar como DONE
    # (opcional, para consultar luego)
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

@app.get("/_debug/tasks")
def debug_tasks():
    return TASKS

@app.get("/_debug/jobs")
def debug_jobs():
    return JOBS
