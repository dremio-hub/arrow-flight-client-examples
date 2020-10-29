"""
  Copyright (C) 2017-2020 Dremio Corporation

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
"""

from functools import wraps

import errno
import os
import signal
import time

import requests

def timeout(seconds, error=os.strerror(errno.ETIMEDOUT)):
    """
    A timeout decorator for readiness check.
    This function returns a decorator that wraps the function to timeout.
    """
    def decorator(func):
        def timeout_handler(signum, frame):
            raise TimeoutError(error)

        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result

        return wraps(func)(wrapper)

    return decorator

# Set timeout to 5 minutes
@timeout(300)
def wait_for_ready():
    """
    A script to poll Dremio's readiness before proceeding to testing.
    """
    while 1:
        try:
            _r = requests.get('http://localhost:9047/')
            if _r.status_code == 200:
                break
        except Exception:
            time.sleep(5)
    print("Dremio server responded with status code 200. Proceeding to testing.")

# Poll for Dremio's readiness.
wait_for_ready()
