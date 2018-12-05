import multiprocessing

bind = "0.0.0.0:8000"
workers = multiprocessing.cpu_count() * 2
worker_class = 'uvicorn.workers.UvicornWorker'
proc_name = 'same-thing'
log_level = 'warning'
