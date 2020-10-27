"""
  Copyright (C) 2017-2018 Dremio Corporation

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

import pytest

from example import connect_to_dremio_flight_server_endpoint

def test_basic_auth():
    """
    Test connection to Dremio.
    """
    connect_to_dremio_flight_server_endpoint("localhost",
      "32010", "dremio", "dremio123", False, False, False, False)

def test_simple_query():
    """
    Test connection to Dremio.
    Then test a simple VALUES query.
    """
    query = "select * from (VALUES(1,2,3))"
    connect_to_dremio_flight_server_endpoint("localhost",
      "32010", "dremio", "dremio123", query, False, False, False)

def test_bad_hostname():
    """
    Test connection with an incorrect server endpoint hostname.
    """
    pytest.xfail("Bad hostname.")
    connect_to_dremio_flight_server_endpoint("badHostNamE",
      "32010", "dremio", "dremio123", False, False, False, False)

def test_bad_port():
    """
    Test connection with an incorrect server endpoint port.
    """
    pytest.xfail("Bad port.")
    connect_to_dremio_flight_server_endpoint("localhost",
      "12345", "dremio", "dremio123", False, False, False, False)

def test_bad_password():
    """
    Test connection with an invalid password.
    """
    pytest.xfail("Bad port.")
    connect_to_dremio_flight_server_endpoint("localhost",
      "32010", "dremio", "badPassword", False, False, False, False)

def test_non_existent_user():
    """
    Test connection with an invalid username.
    """
    pytest.xfail("Non-existent user.")
    connect_to_dremio_flight_server_endpoint("localhost",
      "32010", "noSuchUser", "dremio123", False, False, False, False)
