# Arrow Flight SQL JDBC OAuth Java Examples

This module shows how to use the Arrow Flight SQL JDBC `19.0.0` driver with OAuth connection properties instead of manually calling Dremio's `/oauth/token` endpoint in application code. It also includes a Dremio Software basic-auth inbound impersonation example. It covers:

- `client-credentials`
- `token-exchange`
- `dremio-impersonation`
- `software-impersonation`

## Prerequisites

- Java 11+
- Maven 3.9+
- A Dremio deployment with Arrow Flight SQL and OAuth enabled
- A Dremio OAuth token endpoint, typically `http://<coordinator>:9047/oauth/token` or `https://<coordinator>/oauth/token`
- For `software-impersonation`: Dremio Software only; requires version > 26.1.9. Use Dremio Software 26.1.10 or later.

## Build

```bash
cd java/flight-sql-jdbc-oauth
mvn clean package
```

The driver requires:

```bash
--add-opens=java.base/java.nio=ALL-UNNAMED
```

## Common Options

All subcommands accept these options:

| Option | Description | Default |
|--------|-------------|---------|
| `--host` | Flight SQL hostname | `localhost` |
| `--port` | Flight SQL port | `32010` |
| `--query` | SQL query to run | `SELECT 1 AS example_value` |
| `--max-rows` | Maximum rows to print | `10` |
| `--use-encryption` | Enable encrypted connection | `false` |
| `--disable-certificate-verification` | Disable TLS server verification | `false` |
| `--tls-root-certs` | PEM file for TLS verification | |
| `--trust-store` | Java trust store path | |
| `--trust-store-password` | Trust store password | |
| `--client-certificate` | Client mTLS certificate path | |
| `--client-key` | Client mTLS key path | |
| `--catalog` | Default catalog | |
| `--oauth-token-uri` | OAuth token endpoint (required) | |
| `--oauth-scope` | OAuth scope | `dremio.all` |
| `--oauth-resource` | RFC 8707 resource indicator | |

`--oauth-token-uri` uses Dremio's REST endpoint, while the JDBC connection itself uses the Flight SQL host and port.

Run `<jar> <subcommand> --help` to see all options for a specific subcommand.
Run `<jar> --help` to list the available subcommands.

Common connection options can also be set with environment variables:

| Environment variable | CLI option |
|----------------------|------------|
| `DREMIO_HOST` | `--host` |
| `DREMIO_PORT` | `--port` |
| `DREMIO_QUERY` | `--query` |
| `DREMIO_USE_ENCRYPTION` | `--use-encryption` |
| `DREMIO_DISABLE_CERTIFICATE_VERIFICATION` | `--disable-certificate-verification` |

## Client Credentials

Additional required options: `--oauth-client-id`, `--oauth-client-secret`

```bash
java --add-opens=java.base/java.nio=ALL-UNNAMED \
  -jar target/java-flight-sql-jdbc-oauth-examples-1.0-SNAPSHOT.jar \
  client-credentials \
  --host localhost --port 32010 \
  --oauth-token-uri http://localhost:9047/oauth/token \
  --oauth-client-id service-user-client-id \
  --oauth-client-secret service-user-client-secret
```

## Token Exchange

Additional required options: `--oauth-subject-token`, `--oauth-subject-token-type`

Optional: `--oauth-actor-token` + `--oauth-actor-token-type` (must be provided together), `--oauth-client-id` + `--oauth-client-secret` (must be provided together), `--oauth-audience`, `--oauth-requested-token-type`

Common Dremio subject token types:

- External JWT: `urn:ietf:params:oauth:token-type:jwt`
- PAT: `urn:ietf:params:oauth:token-type:dremio:personal-access-token`

Example using an external JWT:

```bash
java --add-opens=java.base/java.nio=ALL-UNNAMED \
  -jar target/java-flight-sql-jdbc-oauth-examples-1.0-SNAPSHOT.jar \
  token-exchange \
  --host localhost --port 32010 \
  --oauth-token-uri http://localhost:9047/oauth/token \
  --oauth-subject-token "$EXTERNAL_JWT" \
  --oauth-subject-token-type urn:ietf:params:oauth:token-type:jwt
```

Example using a PAT:

```bash
java --add-opens=java.base/java.nio=ALL-UNNAMED \
  -jar target/java-flight-sql-jdbc-oauth-examples-1.0-SNAPSHOT.jar \
  token-exchange \
  --host localhost --port 32010 \
  --oauth-token-uri http://localhost:9047/oauth/token \
  --oauth-subject-token "$DREMIO_PAT" \
  --oauth-subject-token-type urn:ietf:params:oauth:token-type:dremio:personal-access-token
```

## Dremio User Impersonation via Token Exchange

Additional required options: `--target-user`, `--proxy-pat`

Optional: `--oauth-client-id` + `--oauth-client-secret`, `--oauth-audience`, `--oauth-requested-token-type`

This scenario also requires an inbound impersonation policy that allows the proxy user behind `--proxy-pat` to impersonate `--target-user`.

The example maps these values to Dremio's impersonation token exchange contract:

- `subject_token_type=urn:ietf:params:oauth:token-type:dremio:subject`
- `actor_token_type=urn:ietf:params:oauth:token-type:dremio:personal-access-token`

```bash
java --add-opens=java.base/java.nio=ALL-UNNAMED \
  -jar target/java-flight-sql-jdbc-oauth-examples-1.0-SNAPSHOT.jar \
  dremio-impersonation \
  --host localhost --port 32010 \
  --oauth-token-uri http://localhost:9047/oauth/token \
  --target-user sharedaccessuser \
  --proxy-pat "$PROXY_USER_PAT"
```

## Dremio Software Inbound Impersonation

Dremio Software only; requires version > 26.1.9. Use Dremio Software 26.1.10 or later.

This example uses Dremio Software username/password authentication with the Arrow Flight SQL JDBC driver and sets the JDBC connection property:

```text
impersonation_target=<target-user>
```

It does not use OAuth token exchange. The command validates inbound impersonation by running this query by default:

```sql
SELECT USER() AS user_name,
       "SESSION_USER"() AS session_user_name,
       "SYSTEM_USER"() AS system_user_name
```

The example prints the returned row and fails if `USER()`, `SESSION_USER()`, or `SYSTEM_USER()` returns anything other than the target user.

Additional options:

| Option | Environment variable | Description | Default |
|--------|----------------------|-------------|---------|
| `--username`, `--proxy-user` | `DREMIO_USERNAME` | Dremio proxy username | `dremio` |
| `--password` | `DREMIO_PASSWORD` | Dremio proxy user password | Required |
| `--target-user` | `DREMIO_TARGET_USER` | Dremio user to impersonate | Required |

### Dremio Software Setup

Before running the example, make sure the target user exists in Dremio Software. Then configure an inbound impersonation policy as an administrator:

```sql
ALTER SYSTEM SET "exec.impersonation.inbound_policies"='[
  {
    proxy_principals:{users:["<proxy_user>"]},
    target_principals:{users:["<target_user>"]}
  }
]'
```

Verify the policy:

```sql
SELECT name, string_val
FROM sys.options
WHERE name = 'exec.impersonation.inbound_policies'
```

See the Dremio inbound impersonation documentation:
https://docs.dremio.com/current/security/rbac/inbound-impersonation/

### Run

```bash
java --add-opens=java.base/java.nio=ALL-UNNAMED \
  -jar target/java-flight-sql-jdbc-oauth-examples-1.0-SNAPSHOT.jar \
  software-impersonation \
  --host automaster.drem.io \
  --port 32010 \
  --use-encryption \
  --username dremio \
  --password "$DREMIO_PASSWORD" \
  --target-user impersonation_target_test
```

Expected successful output:

```text
Scenario: software-impersonation
Flight SQL URL: jdbc:arrow-flight-sql://automaster.drem.io:32010
Query: SELECT USER() AS user_name, "SESSION_USER"() AS session_user_name, "SYSTEM_USER"() AS system_user_name
user_name	session_user_name	system_user_name
impersonation_target_test	impersonation_target_test	impersonation_target_test
Impersonation validation passed: USER(), SESSION_USER(), and SYSTEM_USER() all returned 'impersonation_target_test'.
```

Expected failure without an inbound impersonation policy:

```text
Proxy user '<proxy>' is not authorized to impersonate target user '<target>'.
```

## Scenarios

- `client-credentials`: Uses `oauth.flow=client_credentials`
- `token-exchange`: Uses `oauth.flow=token_exchange`
- `dremio-impersonation`: Uses `oauth.flow=token_exchange` with Dremio-specific subject and actor token types
- `software-impersonation`: Uses username/password authentication with `impersonation_target=<target-user>`. Dremio Software only; requires version > 26.1.9.
