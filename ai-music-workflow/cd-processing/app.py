"""
AI Music Metadata Project — Web UI Backend
Run: python3 app.py
Access: http://localhost:8000 or http://192.168.1.115:8000
"""

import os, sys, json, asyncio, re
from datetime import datetime
from pathlib import Path
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI(title="AI Music Metadata Project")
BASE_DIR = Path(__file__).parent
OUTPUT_BASE = BASE_DIR / "cd-output-folders"
OPS_DIR = Path(os.environ.get("AI_MUSIC_OPERATIONS_DIR", ""))

@app.get("/", response_class=HTMLResponse)
async def root():
    ui_path = BASE_DIR / "ui.html"
    if ui_path.exists(): return HTMLResponse(ui_path.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>ui.html not found</h1>")

@app.get("/api/config")
async def get_config():
    try:
        sys.path.insert(0, str(BASE_DIR))
        from cd_workflow_config import FILE_PATHS, MODEL_CONFIGS, PROCESSING_THRESHOLDS
        images_folder = FILE_PATHS.get("images_folder", "")
        full_images_path = BASE_DIR / images_folder
        all_images = []
        if full_images_path.exists():
            for ext in ["*.jpg","*.jpeg","*.png"]: all_images += list(full_images_path.glob(ext))
        cd_count = len([f for f in all_images if any(f.name.endswith(x) for x in ["a.jpg","a.jpeg","a.png"])])
        return {
            "images_folder": images_folder,
            "images_folder_exists": full_images_path.exists(),
            "total_images": len(all_images),
            "estimated_cds": cd_count,
            "step1_model": MODEL_CONFIGS.get("step1_metadata_extraction",{}).get("model",""),
            "step3_model": MODEL_CONFIGS.get("step3_ai_analysis",{}).get("model",""),
            "batch_threshold": MODEL_CONFIGS.get("step1_metadata_extraction",{}).get("batch_threshold",10),
            "confidence_threshold": PROCESSING_THRESHOLDS.get("confidence",{}).get("high_confidence",70),
            "library_code": os.environ.get("ALMA_LIBRARY_CODE",""),
            "location_code": os.environ.get("ALMA_LOCATION_CODE",""),
            "env_check": {
                "openai": bool(os.environ.get("OPENAI_API_KEY")),
                "oclc_id": bool(os.environ.get("OCLC_CLIENT_ID")),
                "alma": bool(os.environ.get("ALMA_SANDBOX_API_KEY")),
                "oclc_symbol": bool(os.environ.get("OCLC_INSTITUTION_SYMBOL")),
            }
        }
    except Exception as e: return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/api/config/images-folder")
async def set_images_folder(data: dict):
    new_folder = data.get("folder","").strip()
    if not new_folder: return JSONResponse({"error":"No folder"}, status_code=400)
    config_path = BASE_DIR / "cd_workflow_config.py"
    content = config_path.read_text(encoding="utf-8")
    pattern = r'"images_folder":\s*"[^"]*"'
    replacement = f'"images_folder": "{new_folder}"'
    new_content = re.sub(pattern, replacement, content)
    config_path.write_text(new_content, encoding="utf-8")
    return {"ok": True, "folder": new_folder}

@app.get("/api/batches")
async def list_batches():
    batches = []
    if not OUTPUT_BASE.exists(): return {"batches": []}
    for folder in sorted(OUTPUT_BASE.glob("results-*"), reverse=True)[:30]:
        deliverables = folder / "deliverables"
        batch = {"id": folder.name, "date": folder.name.replace("results-",""),
                 "path": str(folder), "stats": {}, "total": 0,
                 "has_original_cataloging": len(list(folder.glob("original-catalog-index-*.html"))) > 0,
                 "has_review_html": len(list(folder.glob("review-index-*.html"))) > 0}
        try:
            if deliverables.exists():
                sf = list(deliverables.glob("cd-workflow-sorting-*.xlsx"))
                if sf:
                    import openpyxl
                    wb = openpyxl.load_workbook(str(max(sf)), read_only=True)
                    ws = wb.active
                    groups = {}
                    for row in ws.iter_rows(min_row=2, values_only=True):
                        g = row[1]
                        if g: groups[g] = groups.get(g,0) + 1
                    wb.close()
                    batch["stats"] = groups
                    batch["total"] = sum(groups.values())
        except Exception: pass
        batches.append(batch)
    return {"batches": batches}

@app.get("/api/batches/{batch_id}/files")
async def get_batch_files(batch_id: str):
    folder = OUTPUT_BASE / batch_id
    deliverables = folder / "deliverables"
    files = []
    if deliverables.exists():
        for f in sorted(deliverables.iterdir()): files.append({"name":f.name,"size":f.stat().st_size,"path":str(f)})
    for f in sorted(folder.glob("*.html")): files.append({"name":f.name,"size":f.stat().st_size,"path":str(f),"is_html":True})
    return {"files": files}

@app.get("/api/batches/{batch_id}/batch-upload-path")
async def get_batch_upload_path(batch_id: str):
    folder = OUTPUT_BASE / batch_id / "deliverables"
    files = list(folder.glob("batch-upload-alma-cd-*.txt")) if folder.exists() else []
    if not files: return {"path": None}
    return {"path": str(max(files))}

@app.get("/api/batches/{batch_id}/oclc-numbers-path")
async def get_oclc_numbers_path(batch_id: str):
    folder = OUTPUT_BASE / batch_id / "deliverables"
    files = list(folder.glob("batch-upload-alma-cd-*.txt")) if folder.exists() else []
    if not files: return {"path": None}
    lines_data = max(files).read_text(encoding="utf-8").strip().split("\n")
    nums = [l.split("|")[0].strip() for l in lines_data if "|" in l]
    if OPS_DIR and OPS_DIR.exists():
        out_dir = OPS_DIR / "oclc-holdings" / "cd"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{batch_id}-oclc-numbers.txt"
        out_path.write_text("\n".join(nums), encoding="utf-8")
        return {"path": str(out_path), "count": len(nums)}
    return {"path": None, "error": "AI_MUSIC_OPERATIONS_DIR not set"}

@app.get("/api/test/oclc")
async def test_oclc():
    try:
        import requests as req
        r = req.post("https://oauth.oclc.org/token",
            data={"grant_type":"client_credentials","scope":"wcapi"},
            auth=(os.environ.get("OCLC_CLIENT_ID",""), os.environ.get("OCLC_SECRET","")), timeout=15)
        return {"ok": r.status_code==200, "status": r.status_code}
    except Exception as e: return {"ok": False, "error": str(e)}

@app.get("/api/test/alma")
async def test_alma():
    try:
        import requests as req
        region = os.environ.get("ALMA_REGION","api-na")
        key = os.environ.get("ALMA_SANDBOX_API_KEY","")
        r = req.get(f"https://{region}.hosted.exlibrisgroup.com/almaws/v1/conf/general",
            headers={"Authorization":f"apikey {key}","Accept":"application/json"}, timeout=15)
        if r.status_code == 200:
            d = r.json()
            return {"ok":True,"environment":d.get("environment_type"),"institution":d.get("institution",{}).get("desc")}
        return {"ok": False, "status": r.status_code}
    except Exception as e: return {"ok": False, "error": str(e)}

@app.websocket("/ws/run/{job_type}")
async def run_job(websocket: WebSocket, job_type: str):
    await websocket.accept()
    async def send(msg, level="info"):
        try: await websocket.send_json({"type":level,"message":msg,"ts":datetime.now().strftime("%H:%M:%S")})
        except Exception: pass
    try:
        params = await websocket.receive_json()
        await send(f"Starting {job_type}...", "system")
        cmd = build_command(job_type, params)
        if not cmd:
            await send(f"Unknown job or missing params: {job_type}", "error")
            await websocket.send_json({"type":"done","code":1})
            return
        await send(f"Running: {cmd[-1]}", "system")
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            cwd=str(BASE_DIR), env={**os.environ,"PYTHONUNBUFFERED":"1"})
        async for line in proc.stdout:
            text = line.decode("utf-8", errors="replace").rstrip()
            if not text: continue
            lo = text.lower()
            level = "error" if any(w in lo for w in ["error","failed","traceback","exception"]) else \
                    "success" if any(w in lo for w in ["success","completed","ok —","created","imported","done"]) else \
                    "warning" if any(w in lo for w in ["warning","skip","dedup","blocked"]) else \
                    "system" if any(w in lo for w in ["step ","starting","batch id","authenticat","processing mode","running"]) else "info"
            await send(text, level)
        await proc.wait()
        code = proc.returncode
        msg = "Completed successfully" if code==0 else "Exited with code "+str(code)
        await send(msg, "success" if code==0 else "error")
        await websocket.send_json({"type":"done","code":code})
    except WebSocketDisconnect: pass
    except Exception as e:
        try: await websocket.send_json({"type":"error","message":str(e)})
        except Exception: pass

def build_command(job_type: str, params: dict) -> list:
    py = sys.executable
    scripts = {
        "validate": [py,"-u",str(BASE_DIR/"step_.5_cd.py")],
        "step1":    [py,"-u",str(BASE_DIR/"step_1_cd.py")],
        "step15":   [py,"-u",str(BASE_DIR/"step_1.5_cd.py")],
        "step2":    [py,"-u",str(BASE_DIR/"step_2_cd.py")],
        "step3":    [py,"-u",str(BASE_DIR/"step_3_cd.py")],
        "step4":    [py,"-u",str(BASE_DIR/"step_4_cd.py")],
        "step5":    [py,"-u",str(BASE_DIR/"step_5_cd.py")],
        "step6":    [py,"-u",str(BASE_DIR/"step_6_cd.py")],
        "step3b":   [py,"-u",str(BASE_DIR/"step_3b_original_cataloging.py")],
        "step3d":   [py,"-u",str(BASE_DIR/"step_3d_original_catalog_alma_import.py")],
    }
    if job_type == "step3c":
        csv = params.get("csv_path","").strip()
        if not csv:
            # Auto-detect: prefer cataloger decisions CSV, fall back to batch-ready TXT
            import glob as _g
            results = sorted(_g.glob(str(BASE_DIR/"cd-output-folders/results-*")))
            if results:
                latest = results[-1]
                # Priority 1: cataloger decisions CSV
                csv_files = sorted(_g.glob(latest+"/deliverables/*decisions*.csv"))
                if not csv_files:
                    csv_files = sorted(_g.glob(latest+"/*decisions*.csv"))
                if csv_files:
                    csv = csv_files[-1]
                else:
                    # Priority 2: batch-ready TXT
                    txt_files = sorted(_g.glob(latest+"/deliverables/original-cataloging-batch-ready-*.txt"))
                    if txt_files:
                        csv = txt_files[-1]
        return [py,"-u",str(BASE_DIR/"step_3c_oclc_original_record.py"),csv,"--yes"] if csv else []
    if job_type == "alma_import":
        txt = params.get("txt_path","").strip()
        if not txt:
            # Auto-detect latest batch upload file
            import glob as _g
            results = sorted(_g.glob(str(BASE_DIR/"cd-output-folders/results-*")))
            if results:
                candidates = sorted(_g.glob(results[-1]+"/deliverables/batch-upload-alma-cd-*.txt"))
                if candidates:
                    txt = candidates[-1]
        return [py,"-u",str(BASE_DIR/"alma_batch_upload_cd.py"),txt,"--yes"] if txt else []
    if job_type == "oclc_holdings":
        np = params.get("numbers_path","")
        fmt = params.get("format","cd")
        return [py,"-u",str(BASE_DIR.parent.parent/"oclc_holdings.py"),np,"--action","set","--format",fmt,"--yes"] if np else []
    return scripts.get(job_type, [])

if __name__ == "__main__":
    import uvicorn
    import socket
    try:
        import subprocess
        ip = subprocess.check_output(["ipconfig","getifaddr","en0"]).decode().strip()
    except:
        ip = "unknown"
    print("\n" + "="*50)
    print("AI Music Metadata UI")
    print("Local:   http://localhost:8000")
    print("Network: http://" + ip + ":8000")
    print("="*50 + "\n")
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False, log_level="info")