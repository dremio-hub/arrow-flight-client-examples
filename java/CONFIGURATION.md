# Flight Client Configuration Guide

The Flight Client now supports multiple configuration sources with a clear priority order. This allows for flexible deployment and development scenarios.

## Configuration Priority Order

Configuration values are resolved in the following order (highest to lowest priority):

1. **Environment Variables** (highest priority)
2. **Properties File**
3. **Command Line Arguments**
4. **Default Values** (lowest priority)

## Configuration Sources

### 1. Environment Variables

Environment variables must be prefixed with `FLIGHT_` and use lowercase names:

```bash
export FLIGHT_HOST=my-dremio-server.com
export FLIGHT_PORT=32010
export FLIGHT_USER=myuser
export FLIGHT_PASS=mypassword
export FLIGHT_TLS=true
export FLIGHT_VERBOSE=true
```

### 2. Properties File

Create a properties file (default: `flight-client.properties`) in the classpath or specify a custom path:

```properties
# Connection settings
host=localhost
port=32010

# Authentication
user=dremio
pass=dremio123

# TLS settings
tls=false
dsv=false

# Query settings
query=SELECT * FROM sys.version

# Verbose output
verbose=true
```

### 3. Command Line Arguments

All existing command line arguments are still supported:

```bash
java -jar flight-client.jar \
  --hostname my-server.com \
  --port 32010 \
  --username myuser \
  --password mypass \
  --tls \
  --verbose
```

### 4. Default Values

Built-in defaults are used when no other source provides a value:
- Host: `localhost`
- Port: `32010`
- TLS: `false`
- Demo mode: `false`

## Configuration Parameters

| Parameter | Environment Variable | Properties Key | Command Line | Default | Description |
|-----------|---------------------|----------------|--------------|---------|-------------|
| Host | `FLIGHT_HOST` | `host` | `--hostname` | `localhost` | Dremio coordinator hostname |
| Port | `FLIGHT_PORT` | `port` | `--port` | `32010` | Dremio flight server port |
| Username | `FLIGHT_USER` | `user` | `--username` | - | Dremio username |
| Password | `FLIGHT_PASS` | `pass` | `--password` | - | Dremio password |
| PAT/Token | `FLIGHT_PAT` | `pat` | `--personalAccessToken` | - | Personal Access Token |
| Query | `FLIGHT_QUERY` | `query` | `--sqlQuery` | - | SQL query to execute |
| Binary Path | `FLIGHT_BINPATH` | `binpath` | `--saveBinaryPath` | - | Path to save query results |
| TLS | `FLIGHT_TLS` | `tls` | `--tls` | `false` | Enable encrypted connection |
| Disable Server Verification | `FLIGHT_DSV` | `dsv` | `--disableServerVerification` | `false` | Disable TLS server verification |
| Keystore Path | `FLIGHT_KEYSTOREPATH` | `keystorepath` | `--keyStorePath` | - | Path to JKS keystore |
| Keystore Password | `FLIGHT_KEYSTOREPASS` | `keystorepass` | `--keyStorePassword` | - | JKS keystore password |
| Demo Mode | `FLIGHT_DEMO` | `demo` | `--runDemo` | `false` | Run demo mode |
| Engine | `FLIGHT_ENGINE` | `engine` | `--engine` | - | Specific engine to run against |
| Project ID | `FLIGHT_PROJECTID` | `projectid` | `--projectId` | - | Dremio Cloud project ID |
| Verbose | `FLIGHT_VERBOSE` | `verbose` | `--verbose` | `false` | Enable verbose output |

## Usage Examples

### Example 1: Using Environment Variables

```bash
# Set environment variables
export FLIGHT_HOST=prod-dremio.company.com
export FLIGHT_PORT=32010
export FLIGHT_USER=analytics_user
export FLIGHT_PASS=secure_password
export FLIGHT_TLS=true
export FLIGHT_VERBOSE=true

# Run with minimal command line
java -jar flight-client.jar --sqlQuery "SELECT COUNT(*) FROM sales"
```

### Example 2: Using Properties File

Create `my-config.properties`:
```properties
host=dev-dremio.company.com
port=32010
user=dev_user
pass=dev_password
tls=false
verbose=true
```

Run with custom properties file:
```bash
java -jar flight-client.jar --configFile my-config.properties --sqlQuery "SELECT * FROM customers LIMIT 10"
```

### Example 3: Mixed Configuration

```bash
# Environment variables for sensitive data
export FLIGHT_PASS=production_password

# Properties file for common settings
echo "host=prod-dremio.company.com" > prod.properties
echo "port=32010" >> prod.properties
echo "user=prod_user" >> prod.properties
echo "tls=true" >> prod.properties

# Command line for specific query
java -jar flight-client.jar \
  --configFile prod.properties \
  --sqlQuery "SELECT * FROM important_table" \
  --verbose
```

### Example 4: Development vs Production

**Development** (`dev.properties`):
```properties
host=localhost
port=32010
user=dremio
pass=dremio123
tls=false
verbose=true
```

**Production** (environment variables):
```bash
export FLIGHT_HOST=prod-cluster.company.com
export FLIGHT_PORT=32010
export FLIGHT_USER=service_account
export FLIGHT_PASS=secure_production_password
export FLIGHT_TLS=true
export FLIGHT_VERBOSE=false
```

## Debugging Configuration

Use the `--verbose` flag to see which configuration sources are being used:

```bash
java -jar flight-client.jar --verbose --runDemo
```

This will output:
```
Configuration Sources:
Environment variables: 3 found
Properties file entries: 5 found
Command line arguments: parsed successfully

Current Configuration:
  Host: prod-server.com (from environment)
  Port: 32010 (from properties)
  User: myuser (from command line)
  ...
```

## Best Practices

1. **Use environment variables for sensitive data** like passwords and tokens
2. **Use properties files for environment-specific settings** that don't change often
3. **Use command line arguments for runtime-specific options** like queries
4. **Enable verbose mode during development** to understand configuration resolution
5. **Validate your configuration** by running with `--verbose` before production deployment

## Migration from Old Configuration

The new configuration system is backward compatible. Existing command line scripts will continue to work without changes. To take advantage of the new features:

1. Move sensitive data to environment variables
2. Create properties files for common settings
3. Use the `--verbose` flag to verify configuration
4. Gradually adopt the priority-based approach
