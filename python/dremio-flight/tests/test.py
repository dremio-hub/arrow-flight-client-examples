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
from argparse import Namespace
from numpy import array, array_equal
from pyarrow.flight import FlightUnauthenticatedError, FlightUnavailableError
from dotenv import load_dotenv
import certifi
import os
import pytest
from dremio.flight.connection import DremioFlightEndpointConnection
from dremio.flight.query import DremioFlightEndpointQuery

load_dotenv()

args_dict = {
    "hostname": os.getenv("DREMIO_HOSTNAME"),
    "port": os.getenv("DREMIO_FLIGHT_PORT"),
    "username": os.getenv("DREMIO_USERNAME"),
    "password": os.getenv("DREMIO_PASSWORD"),
    "query": "select * from (VALUES(1,2,3))",
    "token": None,
    "tls": False,
    "disable_certificate_verification": True,
    "path_to_certs": certifi.where(),
    "session_properties": None,
    "engine": None,
}

args_dict_ssl = {
    "hostname": os.getenv("DREMIO_CLOUD_HOSTNAME"),
    "port": os.getenv("DREMIO_CLOUD_PORT"),
    "username": None,
    "password": None,
    "query": "select * from (VALUES(1,2,3))",
    "token": os.getenv("DREMIO_PAT"),
    "tls": True,
    "disable_certificate_verification": False,
    "path_to_certs": certifi.where(),
    "session_properties": None,
    "engine": None,
}


args_namespace = Namespace(**args_dict)
args_namespace_ssl = Namespace(**args_dict_ssl)


def test_basic_auth():
    """
    Test connection to Dremio.
    """
    dremio_flight_conn = DremioFlightEndpointConnection(args_namespace)
    flight_client = dremio_flight_conn.connect()
    assert flight_client


def test_simple_query():
    """
    Test connection to Dremio.
    Then test a simple VALUES query.
    """

    dremio_flight_conn = DremioFlightEndpointConnection(args_namespace)
    flight_client = dremio_flight_conn.connect()
    print(f"Flight Client is: {flight_client}")
    dremio_flight_query = DremioFlightEndpointQuery(
        args_namespace.query, flight_client, dremio_flight_conn
    )
    dataframe = dremio_flight_query.execute_query()
    dataframe_arr = dataframe.to_numpy()
    expected_arr = array([[1, 2, 3]])
    assert array_equal(dataframe_arr, expected_arr)


def test_tls():
    """Test connection to Dremio Cloud on a encrypted servers.
    Then test a simple VALUES query.
    """

    dremio_flight_conn = DremioFlightEndpointConnection(args_namespace_ssl)
    flight_client = dremio_flight_conn.connect()
    assert flight_client
    dremio_flight_query = DremioFlightEndpointQuery(
        args_namespace_ssl.query, flight_client, dremio_flight_conn
    )
    dataframe = dremio_flight_query.execute_query()
    dataframe_arr = dataframe.to_numpy()
    expected_arr = array([[1, 2, 3]])
    assert array_equal(dataframe_arr, expected_arr)


def test_bad_hostname():
    """
    Test connection with an incorrect server endpoint hostname.
    """
    args_dict_copy = args_dict.copy()
    args_dict_copy["hostname"] = "ha-ha!"
    args_namespace_modified = Namespace(**args_dict_copy)

    dremio_flight_conn = DremioFlightEndpointConnection(args_namespace_modified)
    with pytest.raises(FlightUnavailableError):
        dremio_flight_conn.connect()


def test_bad_port():
    """
    Test connection with an incorrect server endpoint port.
    """
    args_dict_copy = args_dict.copy()
    args_dict_copy["port"] = 12345
    args_namespace_modified = Namespace(**args_dict_copy)

    dremio_flight_conn = DremioFlightEndpointConnection(args_namespace_modified)
    with pytest.raises(FlightUnavailableError):
        dremio_flight_conn.connect()


def test_bad_password():
    """
    Test connection with an invalid password.
    """
    args_dict_copy = args_dict.copy()
    args_dict_copy["password"] = "ha-ha!"
    args_namespace_modified = Namespace(**args_dict_copy)

    dremio_flight_conn = DremioFlightEndpointConnection(args_namespace_modified)
    with pytest.raises(FlightUnauthenticatedError):
        dremio_flight_conn.connect()


def test_non_existent_user():
    """
    Test connection with an invalid username.
    """
    args_dict_copy = args_dict.copy()
    args_dict_copy["username"] = "ha-ha!"
    args_namespace_modified = Namespace(**args_dict_copy)

    dremio_flight_conn = DremioFlightEndpointConnection(args_namespace_modified)
    with pytest.raises(FlightUnauthenticatedError):
        dremio_flight_conn.connect()
