from typing import TypeVar, Callable, cast, Any
from functools import wraps
from time import perf_counter

from .logger import get_logger

TCallable = TypeVar("TCallable", bound=Callable[..., Any])

logger = get_logger(__name__)

def timed(fn: TCallable) -> TCallable:
    """Simmple decorator writing to the log file elapsed running time of the decorated function."""    
    @wraps(fn)
    def inner(*args, **kwargs):             
        start = perf_counter()
        result = fn(*args, **kwargs)
        total_elapsed = perf_counter() - start
        logger.info(f'Elapsed time for function {fn.__name__}: {round(total_elapsed,2)} seconds')
        return result
    return cast(TCallable, inner)