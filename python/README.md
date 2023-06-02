# Python Arrow Flight Client Application Example

## Quickstart

1. Install [Python 3](https://www.python.org/downloads/)
1. Download and install the [dremio-flight-endpoint whl file](https://github.com/dremio-hub/arrow-flight-client-examples/releases)
    - `python -m pip install <PATH TO WHEEL>`
1. Create a local folder to store the client file and config file.
    1. Create a file named `example.py` in created folder. Copy the contents of `arrow-flight-client-examples/python/example.py` into `example.py`.
    1. Create a file named `config.yaml` in created folder. Copy the contents of `arrow-flight-client-examples/python/config_template.yaml` into `config.yaml`.
    1. Uncomment the options in `config.yaml` as needed, appending the argument after the key. ie. `username: my_username`. You can either delete the options that are not being used or leave them commented.
        - e.g. If you are connecting to a local instance of Dremio, your config file would look like:
        ```
        username: my_username
        password: my_password
        query: SELECT 1
        ```
1. Run the Python Arrow Flight Client by navigating to the created folder in the previous step and running the command `python3 example.py`.

## How to connect to Dremio Cloud

Get started with your first query to Dremio Cloud.

-   The following example requires you to create a [Personal Access Token](https://docs.dremio.com/cloud/security/authentication/personal-access-token/) in Dremio. Replace `<INSERT PAT HERE>` in the example below with your actual PAT token.
-   You may need to wait for a Dremio engine to start up or start it manually if no Dremio engine for your Organization is running.

This example config queries the Dremio Sample dataset `NYC-taxi-trips` and returns the first 10 values.

```
hostname: data.dremio.cloud
port: 443
pat: <INSERT PAT HERE>
tls: True
query: SELECT * FROM Samples."samples.dremio.com"."NYC-taxi-trips" limit 10
```

Run the query using `python3 example.py` and you have now run your first Flight query on Dremio Cloud!

## Configuration Options

### Command Line Arguments

```
usage: example.py [-config CONFIG_REL_PATH]

options:
  -config CONFIG_REL_PATH, --config-path CONFIG_REL_PATH            Set the relative path to the config file. Default is "./config.yaml".
```

### Config File Options

**File Format**

This example's config file uses the [YAML](https://yaml.org/) format.

**Options**

```
hostname: <str>
port: <int>
username: <str>
password: <str>
token: <str>
query: <str>
tls: <bool>
disable_certificate_verification: <bool>
path_to_certs: <str>
session_properties:
  - <str>
  - <str>
engine: <str>
```

`hostname`

_type:_ string

_required:_ False

_default:_ localhost

_description:_ Dremio co-ordinator hostname.

`port`

_type:_ int

_required:_ False

_default:_ 32010

_description:_ Dremio flight server port.

`username`

_type:_ string

_required:_ Required for Dremio Software, not required for Dremio Cloud.

_description:_ Dremio username. Not applicable when connecting to Dremio Cloud.

`password`

_type:_ string

_required:_ Not required for Dremio Cloud, required if no token is provided for Dremio Software.

_description:_ Dremio password. Applicable when connecting to Dremio Software and not applicable when connecting to Dremio Cloud.

`token`

_type:_ string

_required:_ Required for Dremio Cloud, required if no password is provided for Dremio Software.

_description:_ Either a Personal Access Token or an OAuth2 Token. Applicable when connecting to Dremio Cloud or Dremio Software. When connecting to Dremio Software, username must also be specified with conjunction with token. When connecting to Dremio Cloud, only token needs to be specified.

`query`

_type:_ string

_required:_ True

_description:_ SQL query to test.

`tls`

_type:_ boolean

_required:_ False

_default:_ False

_description:_ Enable encrypted connection.

`disable_certificate_verification`

_type:_ boolean

_required:_ False

_default:_ False

_description:_ Disables TLS server verification.

`path_to_certs`

_type:_ string

_required:_ False

_default:_ System Certificates

_description:_ Path to trusted certificates for encrypted connection.

`session_properties`

_type:_ list of strings

_required:_ False

_description:_ Key value pairs of SessionProperty, example:

```
session_properties:
  - schema='Samples."samples.dremio.com"
```

`engine`

_type:_ string

_required:_ False

_description:_ The specific engine to run against. Only applicable to Dremio Cloud.

## Description

![Build Status](https://github.com/dremio-hub/arrow-flight-client-examples/workflows/python-build/badge.svg)

This lightweight Python client application connects to the Dremio Arrow Flight server endpoint. Developers can use token based or regular user credentials (username/password) for authentication. Please note username/password is not supported for Dremio Cloud. Dremio Cloud requires a token. Any datasets in Dremio that are accessible by the provided Dremio user can be queried. Developers can change settings by providing options in a config yaml file before running the client.

Moreover, the tls option can be provided to establish an encrypted connection.
