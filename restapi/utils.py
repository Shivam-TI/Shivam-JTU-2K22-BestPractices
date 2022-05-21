import time
from functools import wraps
import urllib.request
from datetime import datetime

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


def sort_by_time_stamp(logs) -> list:
    data = []
    for log in logs:
        data.append(log.split(" "))
    # print(data)
    data = sorted(data, key=lambda elem: elem[1])
    return data

def response_format(raw_data) -> list:
    response = []
    for timestamp, data in raw_data.items():
        entry = {'timestamp': timestamp}
        logs = []
        data = {k: data[k] for k in sorted(data.keys())}
        for exception, count in data.items():
            logs.append({'exception': exception, 'count': count})
        entry['logs'] = logs
        response.append(entry)
    return response

def aggregate(cleaned_logs) -> dict:
    data = {}
    for log in cleaned_logs:
        [key, text] = log
        value = data.get(key, {})
        value[text] = value.get(text, 0)+1
        data[key] = value
    return data


def transform(logs) -> list:
    result = []
    for log in logs:
        [_, timestamp, text] = log
        text = text.rstrip()
        timestamp = datetime.utcfromtimestamp(int(int(timestamp)/1000))
        hours, minutes = timestamp.hour, timestamp.minute
        key = ''

        if minutes >= 45:
            if hours == 23:
                key = "{:02d}:45-00:00".format(hours)
            else:
                key = "{:02d}:45-{:02d}:00".format(hours, hours+1)
        elif minutes >= 30:
            key = "{:02d}:30-{:02d}:45".format(hours, hours)
        elif minutes >= 15:
            key = "{:02d}:15-{:02d}:30".format(hours, hours)
        else:
            key = "{:02d}:00-{:02d}:15".format(hours, hours)

        result.append([key, text])
        print(key)

    return result


def reader(url, timeout):
    """Read file through HTTP"""
    with urllib.request.urlopen(url, timeout=timeout) as conn:
        return conn.read()


def multi_threaded_reader(urls, num_threads) -> list:
    """Read multiple files through HTTP"""
    result = []
    for url in urls:
        data = reader(url, 60)
        data = data.decode('utf-8')
        result.extend(data.split("\n"))
    result = sorted(result, key=lambda elem:elem[1])
    return result
		