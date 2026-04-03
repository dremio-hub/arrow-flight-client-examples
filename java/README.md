# Java Arrow Flight Examples

This directory is the entrypoint for the Java examples in this repository.

## Available Examples

- [flight-client](flight-client/README.md): Arrow Flight RPC Java sample using `flight-core`
- [flight-sql-jdbc-oauth](flight-sql-jdbc-oauth/README.md): Java 11 Arrow Flight SQL JDBC examples for OAuth client credentials, token exchange, and Dremio user impersonation

## Build Entry Points

- Legacy Flight client: `cd java/flight-client`
- Flight SQL JDBC OAuth examples: `cd java/flight-sql-jdbc-oauth`

## Notes

- `java/pom.xml` is an aggregator for both Java modules.
- The child modules keep separate Maven configuration so they can evolve independently.
