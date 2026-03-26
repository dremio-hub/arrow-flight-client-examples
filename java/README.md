# Java Arrow Flight Examples

This directory is the entrypoint for the Java examples in this repository.

## Available Examples

- [flight-client](flight-client/README.md): Java 8 Arrow Flight RPC sample using `flight-core`
- [flight-sql-jdbc-oauth](flight-sql-jdbc-oauth/README.md): Java 11 Arrow Flight SQL JDBC examples for OAuth client credentials, token exchange, and Dremio user impersonation

## Build Entry Points

- Legacy Flight client: `cd java/flight-client`
- Flight SQL JDBC OAuth examples: `cd java/flight-sql-jdbc-oauth`

## Notes

- `java/pom.xml` is an aggregator for both Java modules.
- The child modules intentionally keep separate Java versions and Maven configuration.
