import os
import queue
import atexit
import weakref
import itertools
import threading
from concurrent.futures import _base


# Using Weak Reference so the garbage collector can remove unused thread
_threads_queues = weakref.WeakKeyDictionary()
# Global variable for when the interpreter is shutting down
_shutdown = False
# Lock to make sure no new workers are
# created while the interpreter is shutting down
_global_shutdown_lock = threading.Lock()


def _python_exit():
    global _shutdown
    with _global_shutdown_lock:
        _shutdown = True
    items = list(_threads_queues.items())
    for t, q in items:
        q.put(None)
    for t, q in items:
        t.join()


# call the function _python_exit when the interpreter exits
atexit.register(_python_exit)


class _WorkItem(object):
    def __init__(self, future: _base.Future, fn, args, kwargs):
        self.future = future
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def run(self):
        if not self.future.set_running_or_notify_cancel():
            return

        try:
            result = self.fn(*self.args, **self.kwargs)
        except Exception as exc:
            self.future.set_exception(exc)
            # Breaking a reference cycle
            self = None
        else:
            self.future.set_result(result)


def _worker(executor_reference, work_queue: queue.Queue, initializer, initargs):
    if initializer is not None:
        try:
            initializer(*initargs)
        except Exception:
            _base.LOGGER.critical('Exception in _worker when calling initializer', exc_info=True)
            executor = executor_reference()
            if executor is not None:
                executor._initializer_failed()
    try:
        while True:
            work_item = work_queue.get(block=True)
            if work_item is not None:
                # print("Running", work_item.args)
                work_item.run()
                # print("Finished", work_item.args)
                # Remove unnecessary reference to an object
                del work_item

                # attempting to increment idle count
                executor = executor_reference()
                if executor is not None:
                    executor._idle_semaphore.release()
                del executor
                continue

            executor = executor_reference()
            # Going to exit only if:
            #   - interpreter is shutting down
            #   - executor that owns the worker has been
            #     collected by the garbage collector, or
            #   - executor that owns the worker has been shutdown.
            if _shutdown or executor is None or executor._shutdown:
                # Flag the executor as shutting down as early as possible
                # if the executor is not collected yet by the garbage collector
                if executor is not None:
                    executor._shutdown = True
                # Notice other workers that either the interpreter
                # or the executor has been shutdown
                work_queue.put(None)
                return
            del executor
    except Exception:
        _base.LOGGER.critical('Exception in _worker function', exc_info=True)


class BrokenThreadPool(RuntimeError):
    """
    Used when a worker thread in a ThreadPoolExecutor failed initializing
    """


class ThreadPoolExecutor(_base.Executor):
    # Used for assigning thread names in case no prefix is supplied
    _counter = itertools.count().__next__

    def __init__(self, max_workers=None, prefix='', initializer=None, initargs=()):
        if max_workers is None:
            # Limiting to only 32 threads to avoid consuming
            # large amount of resources on many core machine
            max_workers = min(32, (os.cpu_count() or 1) + 4)
        print(max_workers)
        if max_workers <= 0:
            raise ValueError("max_worker can't be 0 or lower")

        if initializer is not None and not callable(initializer):
            raise TypeError("initializer must be a callable")

        self._max_workers = max_workers
        self._work_queue = queue.Queue()
        self._idle_semaphore = threading.Semaphore(0)
        self._threads = set()
        self._broken = False
        self._shutdown = False
        self._shutdown_lock = threading.Lock()
        self._prefix = (prefix or f"ThreadPoolExecutor_{self._counter()}")
        self._initializer = initializer
        self._initargs = initargs

    def submit(self, fn, *args, **kwargs):
        with self._shutdown_lock, _global_shutdown_lock:
            if self._broken:
                raise BrokenThreadPool(self._broken)
            if self._shutdown:
                raise RuntimeError("cannot schedule new futures after shutdown")
            if _shutdown:
                raise RuntimeError("cannot schedule new futures after interpreter shutdown")

            f = _base.Future()
            w = _WorkItem(f, fn, args, kwargs)

            self._work_queue.put(w)
            self._adjust_thread_count()
            return f

    def _adjust_thread_count(self):
        # if idle threads are available, don't create a new threads
        if self._idle_semaphore.acquire(timeout=0):
            return

        # When the executor gets lost,
        # the Weak Reference callback will wakeup the worker threads
        def weakref_cb(_, q=self._work_queue):
            q.put(None)

        num_threads = len(self._threads)
        # create new threads worker if the current num of threads
        # is less than the _max_workers
        if num_threads < self._max_workers:
            thread_name = f"{self._prefix or self, num_threads}"
            t = threading.Thread(
                name=thread_name,
                target=_worker,
                args=(
                    weakref.ref(self, weakref_cb),
                    self._work_queue,
                    self._initializer,
                    self._initargs,
                )
            )
            t.start()
            self._threads.add(t)
            _threads_queues[t] = self._work_queue

    def _initializer_failed(self):
        with self._shutdown_lock:
            self.broken = "A thread initializer failed, thread pool not usable anymore"
            while True:
                try:
                    work_item = self._work_queue.get_nowait()
                except queue.Empty:
                    break
                if work_item is not None:
                    work_item.future.set_exception(BrokenThreadPool(self._broken))

    def shutdown(self, wait=True, *, cancel_futures=False):
        with self._shutdown_lock:
            self._shutdown = True
            if cancel_futures:
                while True:
                    try:
                        work_item = self._work_queue.get_nowait()
                    except queue.Empty:
                        break
                    if work_item is not None:
                        work_item.future.cancel()
            # Send a wake-up signal to prevent threads calling
            # `_work_queue.get(block=True)` from permanently blocking
            self._work_queue.put(None)
        if wait:
            for t in self._threads:
                t.join()
