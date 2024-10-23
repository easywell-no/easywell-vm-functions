Cheat Sheet for Running api_server.py on Your Droplet

Copy code
cd easywell-vm-functions

Copy code
source venv/bin/activate && cd scraper_functions

Copy code
nohup uvicorn api_server:app --host 0.0.0.0 --port 8000 > api_server.out 2>&1 &

Explanation:
nohup: Prevents the process from being terminated after you log out.
uvicorn api_server:app: Runs your FastAPI app.
--host 0.0.0.0: Makes the server externally visible.
--port 8000: Runs the server on port 8000.
> api_server.out 2>&1: Redirects both stdout and stderr to api_server.out.
&: Runs the process in the background.