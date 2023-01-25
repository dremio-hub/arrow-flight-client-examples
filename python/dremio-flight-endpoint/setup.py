"""
  Copyright (C) 2017-2021 Dremio Corporation

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
from setuptools import setup, find_namespace_packages

name = "dremio-flight-endpoint"
description = "Package that helps connect and query Dremio's Flight endpoint"
version = "2.0.0"

setup(
    name="flight",
    version=version,
    description=description,
    author="Dremio",
    url="https://github.com/dremio-hub/arrow-flight-client-examples/tree/main/python",
    packages=find_namespace_packages(include=["arguments", "middleware", "flight"]),
    install_requires=["certifi", "pandas", "pyarrow"],
)
