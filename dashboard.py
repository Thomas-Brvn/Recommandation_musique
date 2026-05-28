#!/usr/bin/env python3
"""
Dashboard de monitoring - Music Recommendation System
Usage: python dashboard.py
Ouvre: http://localhost:8080
"""

import asyncio
import json
import os
import subprocess
import sys
import threading
from pathlib import Path
from typing import List, Optional

from google.cloud import storage, compute_v1
from google.api_core.exceptions import NotFound
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import uvicorn

# Configuration
GCP_PROJECT      = os.getenv("GCP_PROJECT_ID", "projetetude-497218")
GCP_REGION       = os.getenv("GCP_REGION", "europe-north1")
GCS_BUCKET_LB    = os.getenv("GCS_BUCKET_RAW_LB", "brainz-raw-listenbrainz")
GCS_BUCKET_MB    = os.getenv("GCS_BUCKET_RAW_MB", "brainz-raw-musicbrainz")
GCS_BUCKET_PROC  = os.getenv("GCS_BUCKET_PROCESSED", "brainz-processed")
BASE_DIR = Path(__file__).parent

app = FastAPI(title="Music Rec Dashboard")

# Global async state
_log_queue: Optional[asyncio.Queue] = None
_main_loop: Optional[asyncio.AbstractEventLoop] = None
_log_clients: List[WebSocket] = []
_pipeline_proc: Optional[subprocess.Popen] = None


@app.on_event("startup")
async def startup():
    global _log_queue, _main_loop
    _log_queue = asyncio.Queue()
    _main_loop = asyncio.get_running_loop()
    asyncio.create_task(_broadcast_worker())


async def _broadcast_worker():
    """Drain log queue and broadcast to all WS clients."""
    while True:
        msg = await _log_queue.get()
        dead = []
        for ws in list(_log_clients):
            try:
                await ws.send_text(json.dumps(msg))
            except Exception:
                dead.append(ws)
        for ws in dead:
            try:
                _log_clients.remove(ws)
            except ValueError:
                pass


def _emit(msg: dict):
    """Thread-safe: send message to all WS clients."""
    if _main_loop and _log_queue:
        asyncio.run_coroutine_threadsafe(_log_queue.put(msg), _main_loop)


def _stream_proc(proc: subprocess.Popen):
    """Background thread: stream subprocess output to WS clients."""
    for line in proc.stdout:
        text = line.rstrip("\n\r")
        if text:
            _emit({"type": "log", "text": text})
    rc = proc.wait()
    _emit({"type": "done", "rc": rc, "text": f"\n--- Processus terminé (code retour: {rc}) ---"})


# ── GCP helpers ────────────────────────────────────────────────────────────────

def _gcs():
    return storage.Client(project=GCP_PROJECT)


def _gce():
    return compute_v1.InstancesClient()


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    html_path = BASE_DIR / "templates" / "dashboard.html"
    return html_path.read_text(encoding="utf-8")


@app.get("/api/status")
async def get_status():
    """Statut général: GCS, GCE, modèle, pipeline."""
    result: dict = {
        "pipeline_running": _pipeline_proc is not None and _pipeline_proc.poll() is None,
        "gcs": {},
        "gce": [],
        "model": {"trained": False},
        "pipeline_completed": False,
        "errors": [],
    }

    # ── GCS ─────────────────────────────────────────────────────────────────
    try:
        gcs = _gcs()
        checks = [
            ("musicbrainz", GCS_BUCKET_MB,   ""),
            ("listenbrainz", GCS_BUCKET_LB,   "incrementals/"),
            ("processed",    GCS_BUCKET_PROC, ""),
            ("models",       GCS_BUCKET_PROC, "models/"),
        ]
        for name, bucket_name, prefix in checks:
            size, count = 0, 0
            for blob in gcs.list_blobs(bucket_name, prefix=prefix):
                size += blob.size or 0
                count += 1
            result["gcs"][name] = {"count": count, "size_gb": round(size / 1e9, 2)}

        # Pipeline completion marker
        try:
            gcs.bucket(GCS_BUCKET_PROC).blob("status/full_pipeline_completed").reload()
            result["pipeline_completed"] = True
        except NotFound:
            pass

        # Model
        try:
            model_blob = gcs.bucket(GCS_BUCKET_PROC).blob("models/als_model.pkl")
            model_blob.reload()
            result["model"]["trained"] = True
            try:
                data = gcs.bucket(GCS_BUCKET_PROC).blob("models/evaluation_results.json").download_as_bytes()
                result["model"]["metrics"] = json.loads(data)
            except Exception:
                pass
        except NotFound:
            pass

    except Exception as e:
        result["errors"].append(f"GCS: {e}")

    # ── GCE ─────────────────────────────────────────────────────────────────
    try:
        gce = _gce()
        zone = f"{GCP_REGION}-a"
        instances = gce.list(project=GCP_PROJECT, zone=zone)
        for inst in instances:
            labels = inst.labels or {}
            if labels.get("project") == "music-recommendation":
                result["gce"].append({
                    "id":           str(inst.id),
                    "name":         inst.name,
                    "type":         inst.machine_type.split("/")[-1],
                    "state":        inst.status,
                })
    except Exception as e:
        result["errors"].append(f"GCE: {e}")

    return result


@app.get("/api/pipeline/steps")
async def get_pipeline_steps():
    """Retourne le statut de chaque étape de la pipeline."""
    gcs = _gcs()

    def check(bucket_name: str, key: str):
        """Retourne {'exists': bool, 'ts': ISO str or None}."""
        try:
            blob = gcs.bucket(bucket_name).blob(key)
            blob.reload()
            return {"exists": True, "ts": blob.updated.isoformat() if blob.updated else None}
        except NotFound:
            return {"exists": False, "ts": None}

    def count_prefix(bucket_name: str, prefix: str) -> int:
        return sum(1 for _ in gcs.list_blobs(bucket_name, prefix=prefix, max_results=1))

    try:
        has_raw   = count_prefix(GCS_BUCKET_LB, "incrementals/") > 0
        dedup     = check(GCS_BUCKET_PROC, "processed/track_dedup_map.json")
        matrix    = check(GCS_BUCKET_PROC, "processed/user_item_matrix.npz")
        model     = check(GCS_BUCKET_PROC, "models/als_model.pkl")
        completed = check(GCS_BUCKET_PROC, "status/full_pipeline_completed")

        steps = [
            {
                "id":    "data",
                "label": "Données brutes",
                "desc":  "Dumps ListenBrainz dans GCS",
                "done":  has_raw,
                "ts":    None,
            },
            {
                "id":    "dedup",
                "label": "Déduplication",
                "desc":  "Mapping des tracks similaires",
                "done":  dedup["exists"],
                "ts":    dedup["ts"],
            },
            {
                "id":    "aggregation",
                "label": "Agrégation",
                "desc":  "Matrice user×track",
                "done":  matrix["exists"],
                "ts":    matrix["ts"],
            },
            {
                "id":    "training",
                "label": "Entraînement ALS",
                "desc":  "Modèle collaboratif",
                "done":  model["exists"],
                "ts":    model["ts"],
            },
            {
                "id":    "done",
                "label": "Pipeline terminé",
                "desc":  "Upload complet sur GCS",
                "done":  completed["exists"],
                "ts":    completed["ts"],
            },
        ]

        # Déterminer l'étape active
        last_done = -1
        for i, s in enumerate(steps):
            if s["done"]:
                last_done = i
        active_idx = last_done + 1 if last_done < len(steps) - 1 else None

        # Vérifier si une instance GCE tourne
        gce = _gce()
        zone = f"{GCP_REGION}-a"
        gce_running = False
        try:
            for inst in gce.list(project=GCP_PROJECT, zone=zone):
                labels = inst.labels or {}
                if (labels.get("project") == "music-recommendation"
                        and inst.status == "RUNNING"):
                    gce_running = True
                    break
        except Exception:
            pass

        for i, s in enumerate(steps):
            s["active"] = (i == active_idx and gce_running)

        return {"steps": steps}
    except Exception as e:
        return {"error": str(e), "steps": []}


@app.get("/api/gce/logs/{instance_name}")
async def get_gce_logs(instance_name: str):
    """Serial port output de l'instance GCE."""
    try:
        serial_client = compute_v1.SerialPortOutputClient()
        zone = f"{GCP_REGION}-a"
        resp = serial_client.get_serial_port_output(
            project=GCP_PROJECT, zone=zone, instance=instance_name
        )
        output = resp.contents or ""
        if not output:
            return {"logs": "Pas encore de logs disponibles — attendre quelques minutes..."}
        lines = output.splitlines()
        return {"logs": "\n".join(lines[-300:])}
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/gce/terminate/{instance_name}")
async def terminate_gce(instance_name: str):
    """Supprime une instance GCE."""
    try:
        gce = _gce()
        zone = f"{GCP_REGION}-a"
        operation = gce.delete(project=GCP_PROJECT, zone=zone, instance=instance_name)
        operation.result()
        _emit({"type": "warn", "text": f"Instance {instance_name} supprimée."})
        return {"ok": True}
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/pipeline/launch")
async def launch_pipeline(body: dict):
    """Lance un pipeline. body: {action: 'full'|'download'|'status'}"""
    global _pipeline_proc

    if _pipeline_proc and _pipeline_proc.poll() is None:
        return {"error": "Un pipeline est déjà en cours d'exécution"}

    action = body.get("action", "full")

    if action == "full":
        cmd = [sys.executable, str(BASE_DIR / "scripts/run_full_pipeline_gce.py")]
    elif action == "status":
        cmd = [sys.executable, str(BASE_DIR / "scripts/run_full_pipeline_gce.py"), "--monitor"]
    elif action == "download":
        cmd = [sys.executable, str(BASE_DIR / "scripts/download_to_gcs_via_gce.py"), "3"]
    else:
        return {"error": f"Action inconnue: {action}"}

    try:
        _pipeline_proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=str(BASE_DIR),
            env={**os.environ},
        )
        threading.Thread(target=_stream_proc, args=(_pipeline_proc,), daemon=True).start()
        _emit({"type": "start", "text": f"--- Lancement: {' '.join(cmd[1:])} ---\n"})
        return {"ok": True, "pid": _pipeline_proc.pid}
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/pipeline/stop")
async def stop_pipeline():
    """Arrête le processus local (ne termine pas l'instance EC2)."""
    global _pipeline_proc
    if _pipeline_proc and _pipeline_proc.poll() is None:
        _pipeline_proc.terminate()
        _emit({"type": "warn", "text": "\n--- Processus local arrêté (l'instance EC2 continue) ---"})
        return {"ok": True}
    return {"error": "Aucun processus actif"}


@app.websocket("/ws/logs")
async def ws_logs(ws: WebSocket):
    """WebSocket pour recevoir les logs en temps réel."""
    await ws.accept()
    _log_clients.append(ws)
    _emit({"type": "info", "text": "Terminal connecté. En attente de logs..."})
    try:
        while True:
            data = await ws.receive_text()
            if data == "ping":
                await ws.send_text(json.dumps({"type": "pong"}))
    except WebSocketDisconnect:
        pass
    finally:
        try:
            _log_clients.remove(ws)
        except ValueError:
            pass


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    print(f"\n  Dashboard: http://localhost:{port}\n")
    uvicorn.run(app, host="0.0.0.0", port=port, reload=False)
