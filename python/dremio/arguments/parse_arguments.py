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

import argparse
import sys
import certifi


class KVParser(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, [])

        for value in values:
            # split it into key and value
            key, value = value.split("=")
            # insert into list as key-value tuples
            getattr(namespace, self.dest).append(
                (key.encode("utf-8"), value.encode("utf-8"))
            )


def parse_arguments():
    """
    Parses the command-line arguments supplied to the script.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-host",
        "--hostname",
        type=str,
        help='Dremio co-ordinator hostname. Defaults to "localhost".',
        default="localhost",
    )
    parser.add_argument(
        "-port",
        "--flightport",
        dest="port",
        type=int,
        help="Dremio flight server port. Defaults to 32010.",
        default=32010,
    )
    parser.add_argument(
        "-user",
        "--username",
        type=str,
        help="Dremio username. Not required when connecting to Dremio Cloud",
        required="-pat" not in sys.argv and "token" not in sys.argv,
    )
    parser.add_argument(
        "-pass",
        "--password",
        type=str,
        help="Dremio password. Not required when connecting to Dremio Cloud",
        required="-pat" not in sys.argv and "token" not in sys.argv,
    )
    parser.add_argument(
        "-pat",
        "--token",
        dest="token",
        type=str,
        help="Either a Personal Access Token or an OAuth2 Token.",
        required="-pass" not in sys.argv and "password" not in sys.argv,
    )
    parser.add_argument(
        "-query",
        "--sqlQuery",
        dest="query",
        type=str,
        help="SQL query to test",
        required=True,
    )
    parser.add_argument(
        "-tls",
        "--tls",
        dest="tls",
        help="Enable encrypted connection. Defaults to False.",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "-dcv",
        "--disableCertificateVerification",
        dest="disable_certificate_verification",
        type=bool,
        help="Disables TLS server verification. Defaults to False.",
        default=False,
    )
    parser.add_argument(
        "-path_to_certs",
        "--trustedCertificates",
        dest="path_to_certs",
        type=str,
        help="Path to trusted certificates for encrypted connection. Defaults to system certificates.",
        default=certifi.where(),
    )
    parser.add_argument(
        "-sp",
        "--sessionProperty",
        dest="session_properties",
        help="Key value pairs of SessionProperty, example: -sp schema='Samples.\"samples.dremio.com\"' -sp key=value",
        required=False,
        nargs="*",
        action=KVParser,
    )
    parser.add_argument(
        "-engine",
        "--engine",
        type=str,
        help="The specific engine to run against. Only applicable to Dremio Cloud.",
        required=False,
    )
    return parser.parse_args()
