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
import yaml
import argparse
import certifi


options_default_validator = {
    "default": {
        "hostname": "localhost",
        "port": 32010,
        "tls": False,
        "disable_certificate_verification": False,
        "path_to_certs": certifi.where(),
    },
    "type": {
        "hostname": str,
        "port": int,
        "username": str,
        "password": str,
        "token": str,
        "query": str,
        "tls": bool,
        "disable_certificate_verification": bool,
        "path_to_certs": str,
        "session_properties": list,
        "engine": str,
    },
    "required": {
        "query": True,
    },
}


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-config",
        "--config-path",
        dest="config_rel_path",
        type=str,
        help="Path to config file. Defaults to config.yaml in the same directory.",
        default="config.yaml",
    )
    return parser.parse_args()


def _encode_key_value(property: dict) -> tuple:
    return tuple(map(lambda x: x.encode("utf_8"), list(property.items())[0]))


def convert_session_properties(session_properties: list[dict]) -> list[tuple]:
    return list(map(_encode_key_value, session_properties))


def parse_config_file(config_rel_path: str):
    with open(config_rel_path, mode="rt") as file:
        try:
            options = yaml.safe_load(file)
            if "session_properties" in options:
                options["session_properties"] = convert_session_properties(
                    options["session_properties"]
                )
            return options
        except yaml.YAMLError as exc:
            raise exc


def validate_options_type(config: dict):
    for key in config:
        value = config[key]
        expected_type_for_key = options_default_validator["type"].get(key)

        if expected_type_for_key is None:
            continue

        if not isinstance(value, expected_type_for_key):
            raise TypeError(
                f"The expected type for {key} is {expected_type_for_key}. You input {value} which is {type(value)}."
            )


def validate_required_options(config: dict):
    for key in options_default_validator["required"]:
        if config.get(key) is None:
            raise Exception(
                f"The '{key}' option is required. Please input a value for '{key}'."
            )


def validate_auth_config(config: dict):
    if config.get("username") is None:
        if config.get("token") is None:
            raise Exception(
                "When connecting to Dremio Cloud, a 'token' must be supplied. If connecting to Dremio Software, a 'username' and ('password' or 'token') must be provided."
            )
    elif config.get("password") is None and config.get("token") is None:
        raise Exception(
            "When connecting to Dremio Cloud, a 'token' must be supplied. When connecting to Dremio Software, a 'username' and ('password' or 'token') must be provided."
        )


def validate_config(config: dict):
    validate_options_type(config)
    validate_required_options(config)
    validate_auth_config(config)
    return


def get_config():
    args = parse_arguments()
    config = options_default_validator["default"] | parse_config_file(
        args.config_rel_path
    )
    validate_config(config=config)
    return config
