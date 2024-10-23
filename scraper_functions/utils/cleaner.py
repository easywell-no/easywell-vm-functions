import os
import psutil
import subprocess
import logging

def clear_system_cache():
    try:
        # Clear PageCache, dentries and inodes
        subprocess.run(["sudo", "sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"], check=True)
        logging.info("System cache cleared successfully")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to clear system cache: {e}")

def clear_python_cache():
    try:
        # Remove __pycache__ directories and .pyc files
        subprocess.run(["find", ".", "-type", "d", "-name", "__pycache__", "-exec", "rm", "-rf", "{}", "+"], check=True)
        subprocess.run(["find", ".", "-name", "*.pyc", "-delete"], check=True)
        logging.info("Python cache cleared successfully")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to clear Python cache: {e}")

def log_memory_usage():
    memory = psutil.virtual_memory()
    logging.info(f"Memory usage: {memory.percent}%")

def cleanup():
    logging.info("Starting cleanup process")
    
    # Log memory usage before cleanup
    log_memory_usage()
    
    # Clear system and Python caches
    clear_system_cache()
    clear_python_cache()
    
    # Log memory usage after cleanup
    log_memory_usage()
    
    logging.info("Cleanup process completed")

if __name__ == "__main__":
    cleanup()
