import multiprocessing

bind = "0.0.0.0:8000"
workers = min(4, multiprocessing.cpu_count())
worker_class = 'uvicorn.workers.UvicornWorker'
proc_name = 'same-thing'
loglevel = 'warning'
