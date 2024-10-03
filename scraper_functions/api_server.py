import os
import subprocess
import threading
import logging
from logging.handlers import RotatingFileHandler
import sys
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv
from typing import Optional
import psutil  # For better process management
import asyncio

# Load environment variables
load_dotenv()

# Configure logging for api_server.py
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(log_dir, exist_ok=True)

log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
log_file = os.path.join(log_dir, "api_server.log")

rotating_handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=5)
rotating_handler.setFormatter(log_formatter)
rotating_handler.setLevel(logging.INFO)

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(log_formatter)
stream_handler.setLevel(logging.INFO)

logging.basicConfig(
    level=logging.INFO,
    handlers=[rotating_handler, stream_handler]
)

app = FastAPI()

# Store the process status in a dictionary
process_status = {
    "generate_report": {"running": False, "pid": None},
    "scrape_and_store": {"running": False, "pid": None}
}

class ReportRequest(BaseModel):
    target_location: str

class ScriptStatus(BaseModel):
    script_name: str

async def read_subprocess_output(process, script_name):
    try:
        stdout, stderr = process.communicate(timeout=300)  # Adjust timeout as needed
        if stdout:
            logging.info(f"Output from {script_name}: {stdout.decode().strip()}")
        if stderr:
            logging.error(f"Error from {script_name}: {stderr.decode().strip()}")
    except subprocess.TimeoutExpired:
        process.kill()
        logging.error(f"{script_name} subprocess timed out and was killed.")

@app.post("/generate_report/")
async def generate_report(request: ReportRequest):
    target = request.target_location
    if process_status["generate_report"]["running"]:
        raise HTTPException(status_code=400, detail="Generate report is already running.")
    
    try:
        # Start the report generation script
        process = subprocess.Popen(
            ["python", "generate_report.py", target],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            env=os.environ.copy(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        # Update the process status
        process_status["generate_report"] = {"running": True, "pid": process.pid}
        # Start monitoring the process
        monitor_thread = threading.Thread(target=monitor_process, args=("generate_report", process.pid))
        monitor_thread.start()
        # Handle subprocess output asynchronously
        asyncio.create_task(read_subprocess_output(process, "generate_report"))
        return {"message": f"Report generation started for {target}.", "pid": process.pid}
    except Exception as e:
        logging.error(f"Failed to start generate_report: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/scrape_and_store/")
async def scrape_and_store():
    if process_status["scrape_and_store"]["running"]:
        raise HTTPException(status_code=400, detail="Scraping and storing is already running.")
    
    try:
        # Start the scraping script
        process = subprocess.Popen(
            ["python", "scrape_and_store.py"],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            env=os.environ.copy(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        # Update the process status
        process_status["scrape_and_store"] = {"running": True, "pid": process.pid}
        # Start monitoring the process
        monitor_thread = threading.Thread(target=monitor_process, args=("scrape_and_store", process.pid))
        monitor_thread.start()
        # Handle subprocess output asynchronously
        asyncio.create_task(read_subprocess_output(process, "scrape_and_store"))
        return {"message": "Scraping and storing started.", "pid": process.pid}
    except Exception as e:
        logging.error(f"Failed to start scrape_and_store: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/script_status/")
async def script_status(status_request: ScriptStatus):
    script_name = status_request.script_name
    if script_name not in process_status:
        raise HTTPException(status_code=400, detail=f"No script named {script_name}.")
    
    status = process_status[script_name]
    return {"running": status["running"], "pid": status["pid"]}

@app.post("/stop_script/")
async def stop_script(status_request: ScriptStatus):
    script_name = status_request.script_name
    if script_name not in process_status:
        raise HTTPException(status_code=400, detail=f"No script named {script_name}.")
    
    status = process_status[script_name]
    if status["running"] and status["pid"]:
        try:
            process = psutil.Process(status["pid"])
            process.terminate()  # Graceful termination
            try:
                process.wait(timeout=10)
                logging.info(f"Process {script_name} terminated gracefully.")
            except psutil.TimeoutExpired:
                process.kill()  # Force kill
                logging.warning(f"Process {script_name} killed forcefully.")
            process_status[script_name] = {"running": False, "pid": None}
            return {"message": f"Process {script_name} stopped."}
        except psutil.NoSuchProcess:
            process_status[script_name] = {"running": False, "pid": None}
            return {"message": f"Process {script_name} does not exist."}
        except Exception as e:
            logging.error(f"Failed to stop {script_name}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to stop {script_name}: {e}")
    else:
        raise HTTPException(status_code=400, detail=f"No running process for {script_name}.")

# Background process monitor to update the process status when the script finishes
def monitor_process(script_name, pid):
    try:
        proc = psutil.Process(pid)
        proc.wait()  # Wait until the process completes
        process_status[script_name] = {"running": False, "pid": None}
        logging.info(f"Process {script_name} with PID {pid} has completed.")
    except psutil.NoSuchProcess:
        process_status[script_name] = {"running": False, "pid": None}
        logging.warning(f"Process {script_name} with PID {pid} does not exist.")
    except Exception as e:
        process_status[script_name] = {"running": False, "pid": None}
        logging.error(f"Failed to monitor process {pid} for {script_name}: {e}")
