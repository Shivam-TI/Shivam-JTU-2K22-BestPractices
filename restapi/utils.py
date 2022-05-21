import time
from functools import wraps

def calculate_time(logger, decorator=None):
	assert callable(decorator)
	def inner_decorator(func):
		@wraps(func)
		def wrapper(*args, **kwargs):
			start = time.time()
			logger.info(f"Function {str(func.__name__)}: started")
			res = func()
			logger.info(f"Function {str(func.__name__)}: fininsed in {int((time.time() - start) * 1000)} ms")
			return res
		return wrapper
	return inner_decorator(decorator) if callable(decorator) else inner_decorator

