from contextlib import contextmanager
from time import perf_counter

@contextmanager
def timed(label: str, results: dict):
    """
    A simple context manager to time a block of code and store the result.
    Args:
        label: A string label for the timed operation.
        results: A dictionary where the timing result will be stored.
    """
    t0 = perf_counter()
    try:
        yield
    finally:
        results[label] = perf_counter() - t0 