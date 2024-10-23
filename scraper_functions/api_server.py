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
import json

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

# Create a lock for thread safety
process_status_lock = threading.Lock()

class ReportRequest(BaseModel):
    latitude: float
    longitude: float

async def read_subprocess_output(process, script_name):
    try:
        stdout, stderr = process.communicate()
        if stdout:
            logging.info(f"Output from {script_name}: {stdout.decode().strip()}")
        if stderr:
            logging.error(f"Error from {script_name}: {stderr.decode().strip()}")
    except Exception as e:
        logging.error(f"Error monitoring {script_name}: {e}")

@app.post("/generate_report/")
async def generate_report(request: ReportRequest):
    latitude = request.latitude
    longitude = request.longitude
    with process_status_lock:
        if process_status["generate_report"]["running"]:
            raise HTTPException(status_code=400, detail="Generate report is already running.")
    try:
        script_paths = {
            "generate_report": os.path.join(os.path.dirname(os.path.abspath(__file__)), "all_generate_report_functions", "generate_report.py")
        }
        # Start the report generation script with latitude and longitude as arguments
        process = subprocess.Popen(
            [sys.executable, script_paths["generate_report"], str(latitude), str(longitude)],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            env=os.environ.copy(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        with process_status_lock:
            process_status["generate_report"] = {"running": True, "pid": process.pid}
        # Start monitoring the process
        monitor_thread = threading.Thread(target=monitor_process, args=("generate_report", process.pid))
        monitor_thread.start()
        # Handle subprocess output asynchronously
        asyncio.create_task(read_subprocess_output(process, "generate_report"))
        return {"message": f"Report generation started for coordinates ({latitude}, {longitude}).", "pid": process.pid}
    except Exception as e:
        logging.error(f"Failed to start generate_report: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/scripts/{script_name}/start/")
async def start_script(script_name: str):
    script_name = script_name.lower()
    valid_scripts = ["scrape_and_store"]
    if script_name not in valid_scripts:
        raise HTTPException(status_code=400, detail=f"Invalid script name '{script_name}'.")
    with process_status_lock:
        if process_status[script_name]["running"]:
            raise HTTPException(status_code=400, detail=f"{script_name} is already running.")
    try:
        # Prepare environment variables
        env = os.environ.copy()
        # Adjust PYTHONPATH
        env["PYTHONPATH"] = os.pathsep.join([
            os.path.dirname(os.path.abspath(__file__)),
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "utils"),
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "all_generate_report_functions"),
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "all_scrape_and_store_functions"),
        ])

        # Start the scraping script
        script_paths = {
            "scrape_and_store": os.path.join(os.path.dirname(os.path.abspath(__file__)), "all_scrape_and_store_functions", "scrape_and_store.py")
        }
        process = subprocess.Popen(
            [sys.executable, script_paths[script_name]],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            start_new_session=True
        )
        with process_status_lock:
            process_status[script_name] = {"running": True, "pid": process.pid}
        # Start monitoring the process
        monitor_thread = threading.Thread(target=monitor_process, args=(script_name, process.pid))
        monitor_thread.start()
        return {"message": f"{script_name} started.", "pid": process.pid}
    except Exception as e:
        logging.error(f"Failed to start {script_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/scripts/{script_name}/status/")
async def get_script_status(script_name: str):
    script_name = script_name.lower()
    with process_status_lock:
        if script_name not in process_status:
            raise HTTPException(status_code=400, detail=f"No script named '{script_name}'.")
        status = process_status[script_name]
    return {"running": status["running"], "pid": status["pid"]}

@app.post("/scripts/{script_name}/stop/")
async def stop_script(script_name: str):
    script_name = script_name.lower()
    with process_status_lock:
        if script_name not in process_status:
            raise HTTPException(status_code=400, detail=f"No script named '{script_name}'.")
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
            with process_status_lock:
                process_status[script_name] = {"running": False, "pid": None}
            return {"message": f"Process {script_name} stopped."}
        except psutil.NoSuchProcess:
            with process_status_lock:
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
    except psutil.NoSuchProcess:
        logging.warning(f"Process {script_name} with PID {pid} does not exist.")
    except Exception as e:
        logging.error(f"Failed to monitor process {pid} for {script_name}: {e}")
    finally:
        with process_status_lock:
            process_status[script_name] = {"running": False, "pid": None}
        logging.info(f"Process {script_name} with PID {pid} has completed.")

@app.get("/database_status/")
async def database_status():
    """
    Endpoint to check the status of the databases.
    Runs the check_database_content.py script and returns its JSON output.
    """
    script_name = "check_database_content.py"
    script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "all_url_helpers", script_name)

    if not os.path.exists(script_path):
        logging.error(f"{script_name} does not exist.")
        raise HTTPException(status_code=500, detail=f"{script_name} does not exist.")

    try:
        # Run the script and capture its output
        process = subprocess.Popen(
            [sys.executable, script_path],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            env=os.environ.copy(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        stdout, stderr = process.communicate(timeout=300)  # Adjust timeout as needed

        if process.returncode != 0:
            logging.error(f"{script_name} failed with error: {stderr.decode().strip()}")
            raise HTTPException(status_code=500, detail=f"{script_name} failed: {stderr.decode().strip()}")

        output = stdout.decode().strip()
        try:
            data = json.loads(output)
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON output from {script_name}: {e}")
            raise HTTPException(status_code=500, detail=f"Invalid JSON output from {script_name}")

        return data

    except subprocess.TimeoutExpired:
        process.kill()
        logging.error(f"{script_name} subprocess timed out and was killed.")
        raise HTTPException(status_code=500, detail=f"{script_name} subprocess timed out and was killed.")
    except Exception as e:
        logging.error(f"Failed to run {script_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
