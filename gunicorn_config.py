import multiprocessing

bind = "0.0.0.0:8000"
workers = multiprocessing.cpu_count() * 2
worker_class = 'gthread'
threads = 10
proc_name = 'same-thing'
