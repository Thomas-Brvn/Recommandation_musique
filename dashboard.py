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

import boto3
from botocore.exceptions import ClientError
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import uvicorn

# Configuration
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "brainz-data")
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


# ── AWS helpers ────────────────────────────────────────────────────────────────

def _s3():
    return boto3.client("s3", region_name=AWS_REGION)


def _ec2():
    return boto3.client("ec2", region_name=AWS_REGION)


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    html_path = BASE_DIR / "templates" / "dashboard.html"
    return html_path.read_text(encoding="utf-8")


@app.get("/api/status")
async def get_status():
    """Statut général: S3, EC2, modèle, pipeline."""
    result: dict = {
        "pipeline_running": _pipeline_proc is not None and _pipeline_proc.poll() is None,
        "s3": {},
        "ec2": [],
        "model": {"trained": False},
        "pipeline_completed": False,
        "errors": [],
    }

    # ── S3 ──────────────────────────────────────────────────────────────────
    try:
        s3 = _s3()
        prefixes = {
            "musicbrainz":            "raw/musicbrainz/",
            "listenbrainz":           "raw/listenbrainz/incrementals/",
            "processed":              "processed/",
            "models":                 "models/",
        }
        for name, prefix in prefixes.items():
            size, count = 0, 0
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
                for obj in page.get("Contents", []):
                    size += obj["Size"]
                    count += 1
            result["s3"][name] = {"count": count, "size_gb": round(size / 1e9, 2)}

        # Pipeline completion marker
        try:
            s3.head_object(Bucket=S3_BUCKET, Key="status/full_pipeline_completed")
            result["pipeline_completed"] = True
        except ClientError:
            pass

        # Model
        try:
            s3.head_object(Bucket=S3_BUCKET, Key="models/als_model.pkl")
            result["model"]["trained"] = True
            try:
                obj = s3.get_object(Bucket=S3_BUCKET, Key="models/evaluation_results.json")
                result["model"]["metrics"] = json.loads(obj["Body"].read())
            except Exception:
                pass
        except ClientError:
            pass

    except Exception as e:
        result["errors"].append(f"S3: {e}")

    # ── EC2 ─────────────────────────────────────────────────────────────────
    try:
        ec2 = _ec2()
        resp = ec2.describe_instances(
            Filters=[
                {"Name": "tag:Project", "Values": ["MusicRecommendation"]},
                {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
            ]
        )
        for reservation in resp["Reservations"]:
            for inst in reservation["Instances"]:
                name = next(
                    (t["Value"] for t in inst.get("Tags", []) if t["Key"] == "Name"),
                    "N/A",
                )
                result["ec2"].append({
                    "id":          inst["InstanceId"],
                    "type":        inst["InstanceType"],
                    "state":       inst["State"]["Name"],
                    "name":        name,
                    "launch_time": inst["LaunchTime"].isoformat(),
                })
    except Exception as e:
        result["errors"].append(f"EC2: {e}")

    return result


@app.get("/api/ec2/logs/{instance_id}")
async def get_ec2_logs(instance_id: str):
    """Console output de l'instance EC2."""
    try:
        ec2 = _ec2()
        resp = ec2.get_console_output(InstanceId=instance_id)
        output = resp.get("Output", "")
        if not output:
            return {"logs": "Pas encore de logs disponibles — attendre quelques minutes..."}
        # Return last ~300 lines
        lines = output.splitlines()
        return {"logs": "\n".join(lines[-300:])}
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/ec2/terminate/{instance_id}")
async def terminate_ec2(instance_id: str):
    """Termine une instance EC2."""
    try:
        ec2 = _ec2()
        ec2.terminate_instances(InstanceIds=[instance_id])
        _emit({"type": "warn", "text": f"Instance {instance_id} terminée."})
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
        cmd = [sys.executable, str(BASE_DIR / "scripts/run_full_pipeline_ec2.py"), "--no-monitor"]
    elif action == "status":
        cmd = [sys.executable, str(BASE_DIR / "scripts/run_full_pipeline_ec2.py"), "--status"]
    elif action == "download":
        # download_to_s3_via_ec2.py needs config/aws_config.json + choice "3"
        cmd = [sys.executable, str(BASE_DIR / "scripts/download_to_s3_via_ec2.py"), "3"]
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
