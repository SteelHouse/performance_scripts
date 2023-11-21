async def bounded_gather(sem, task_func, *args):
    await sem.acquire()  # Try to acquire the semaphore
    try:
        result = await task_func(*args)
        return result
    finally:
        sem.release()  # Release the semaphore so that another task can start
