import argparse
import certifi


class KVParser(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, list())

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
        help='Dremio username. Defaults to "dremio".',
        default="dremio",
    )
    parser.add_argument(
        "-pass",
        "--password",
        type=str,
        help='Dremio password. Defaults to "dremio123".',
        default="dremio123",
    )
    parser.add_argument(
        "-pat",
        "--personalAccessToken",
        "-authToken",
        "--authToken",
        dest="token",
        type=str,
        help="Either a Personal Access Token or an OAuth2 Token.",
        required=False,
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
        "-ecv",
        "--enableCertificateVerification",
        dest="enable_certificate_verification",
        type=bool,
        help="Enables or disables TLS server verification. Defaults to true.",
        default=True,
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
        help="The specific engine to run against.",
        required=False,
    )
    return parser.parse_args()
