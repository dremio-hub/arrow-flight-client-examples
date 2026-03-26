# Arrow Flight SQL JDBC OAuth Java Examples

This module shows how to use the Arrow Flight SQL JDBC `19.0.0` driver with OAuth connection properties instead of manually calling Dremio's `/oauth/token` endpoint in application code. It covers:

- `client-credentials`
- `token-exchange`
- `dremio-impersonation`

## Prerequisites

- Java 11+
- Maven 3.9+
- A Dremio deployment with Arrow Flight SQL and OAuth enabled
- A Dremio OAuth token endpoint, typically `http://<coordinator>:9047/oauth/token` or `https://<coordinator>/oauth/token`

## Build

```bash
cd java/flight-sql-jdbc-oauth
mvn clean package
```

The driver requires:

```bash
--add-opens=java.base/java.nio=ALL-UNNAMED
```

## Common Environment Variables

- `DREMIO_HOST`: Flight SQL hostname
- `DREMIO_FLIGHT_PORT`: Flight SQL port, defaults to `32010`
- `DREMIO_SQL`: Query to run, defaults to `SELECT 1 AS example_value`
- `DREMIO_MAX_ROWS`: Maximum rows to print, defaults to `10`
- `DREMIO_USE_ENCRYPTION`: `true` or `false`, defaults to `false`
- `DREMIO_DISABLE_CERTIFICATE_VERIFICATION`: `true` or `false`, defaults to `false`
- `DREMIO_TLS_ROOT_CERTS`: Optional PEM file for TLS verification
- `DREMIO_TRUST_STORE`: Optional Java trust store path
- `DREMIO_TRUST_STORE_PASSWORD`: Optional trust store password
- `DREMIO_CLIENT_CERTIFICATE`: Optional client mTLS certificate path
- `DREMIO_CLIENT_KEY`: Optional client mTLS key path
- `DREMIO_CATALOG`: Optional default catalog
- `DREMIO_OAUTH_TOKEN_URI`: OAuth token endpoint used by the driver
- `DREMIO_OAUTH_SCOPE`: Optional scope, defaults to `dremio.all`
- `DREMIO_OAUTH_RESOURCE`: Optional RFC 8707 resource indicator

`DREMIO_OAUTH_TOKEN_URI` uses Dremio's REST endpoint, while the JDBC connection itself uses the Flight SQL host and port.

## Client Credentials

Required:

- `DREMIO_OAUTH_CLIENT_ID`
- `DREMIO_OAUTH_CLIENT_SECRET`

Example:

```bash
export DREMIO_HOST=localhost
export DREMIO_FLIGHT_PORT=32010
export DREMIO_OAUTH_TOKEN_URI=http://localhost:9047/oauth/token
export DREMIO_OAUTH_CLIENT_ID=service-user-client-id
export DREMIO_OAUTH_CLIENT_SECRET=service-user-client-secret

java --add-opens=java.base/java.nio=ALL-UNNAMED \
  -jar target/java-flight-sql-jdbc-oauth-examples-1.0-SNAPSHOT.jar \
  client-credentials
```

## Token Exchange

Required:

- `DREMIO_OAUTH_SUBJECT_TOKEN`
- `DREMIO_OAUTH_SUBJECT_TOKEN_TYPE`

Optional:

- `DREMIO_OAUTH_ACTOR_TOKEN`
- `DREMIO_OAUTH_ACTOR_TOKEN_TYPE`
- `DREMIO_OAUTH_AUDIENCE`
- `DREMIO_OAUTH_REQUESTED_TOKEN_TYPE`
- `DREMIO_OAUTH_CLIENT_ID`
- `DREMIO_OAUTH_CLIENT_SECRET`

Common Dremio subject token types:

- External JWT: `urn:ietf:params:oauth:token-type:jwt`
- PAT: `urn:ietf:params:oauth:token-type:dremio:personal-access-token`

Example using an external JWT:

```bash
export DREMIO_HOST=localhost
export DREMIO_FLIGHT_PORT=32010
export DREMIO_OAUTH_TOKEN_URI=http://localhost:9047/oauth/token
export DREMIO_OAUTH_SUBJECT_TOKEN="$EXTERNAL_JWT"
export DREMIO_OAUTH_SUBJECT_TOKEN_TYPE=urn:ietf:params:oauth:token-type:jwt

java --add-opens=java.base/java.nio=ALL-UNNAMED \
  -jar target/java-flight-sql-jdbc-oauth-examples-1.0-SNAPSHOT.jar \
  token-exchange
```

Example using a PAT:

```bash
export DREMIO_OAUTH_SUBJECT_TOKEN="$DREMIO_PAT"
export DREMIO_OAUTH_SUBJECT_TOKEN_TYPE=urn:ietf:params:oauth:token-type:dremio:personal-access-token
```

## Dremio User Impersonation via Token Exchange

Required:

- `DREMIO_TARGET_USER`
- `DREMIO_PROXY_PAT`

This scenario also requires an inbound impersonation policy that allows the proxy user behind `DREMIO_PROXY_PAT` to impersonate `DREMIO_TARGET_USER`.

The example maps these values to Dremio's impersonation token exchange contract:

- `subject_token_type=urn:ietf:params:oauth:token-type:dremio:subject`
- `actor_token_type=urn:ietf:params:oauth:token-type:dremio:personal-access-token`

Example:

```bash
export DREMIO_HOST=localhost
export DREMIO_FLIGHT_PORT=32010
export DREMIO_OAUTH_TOKEN_URI=http://localhost:9047/oauth/token
export DREMIO_TARGET_USER=sharedaccessuser
export DREMIO_PROXY_PAT="$PROXY_USER_PAT"

java --add-opens=java.base/java.nio=ALL-UNNAMED \
  -jar target/java-flight-sql-jdbc-oauth-examples-1.0-SNAPSHOT.jar \
  dremio-impersonation
```

## Scenarios

- `client-credentials`: Uses `oauth.flow=client_credentials`
- `token-exchange`: Uses `oauth.flow=token_exchange`
- `dremio-impersonation`: Uses `oauth.flow=token_exchange` with Dremio-specific subject and actor token types
