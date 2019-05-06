import multiprocessing

from same_thing.db import purge_data_dbs

bind = "0.0.0.0:8000"
workers = min(4, multiprocessing.cpu_count())
worker_class = 'uvicorn.workers.UvicornWorker'
proc_name = 'same-thing'
loglevel = 'warning'


def on_starting(server):
    purge_data_dbs()
