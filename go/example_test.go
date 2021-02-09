package main

import (
	"bytes"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDremio(t *testing.T) {
	tests := []struct {
		name     string
		argList  []string
		expected string
	}{
		// uses defaults for host and port, localhost and 32010
		{"basic auth", []string{"--user=dremio", "--pass=dremio123"}, "[INFO] Authentication was successful."},
		// set host and port and send a query
		{"test simple query", []string{"--host=localhost", "--port=32010", "--user=dremio", "--pass=dremio123",
			"--query", "SELECT * FROM (VALUES(1,2,3))"},
			`[INFO] Authentication was successful.
[INFO] Query: SELECT * FROM (VALUES(1,2,3))
[INFO] GetSchema was successful.
[INFO] Schema: schema:
  fields: 3
    - EXPR$0: type=int64, nullable
    - EXPR$1: type=int64, nullable
    - EXPR$2: type=int64, nullable
[INFO] GetFlightInfo was successful.
[INFO] Reading query results from dremio.
record:
  schema:
  fields: 3
    - EXPR$0: type=int64, nullable
    - EXPR$1: type=int64, nullable
    - EXPR$2: type=int64, nullable
  rows: 1
  col[0][EXPR$0]: [1]
  col[1][EXPR$1]: [2]
  col[2][EXPR$2]: [3]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Args = append([]string{os.Args[0]}, tt.argList...)
			var buf bytes.Buffer
			log.SetFlags(0)
			log.SetOutput(&buf)
			defer log.SetOutput(os.Stderr)
			main()

			assert.Equal(t, tt.expected, strings.TrimSpace(buf.String()))
		})
	}
}

func TestErrors(t *testing.T) {
	tests := []struct {
		name     string
		argList  []string
		expected string
	}{
		{"bad hostname", []string{"--user=dremio", "--pass=dremio123", "--host=badHostNamE"},
			`rpc error: code = Unavailable desc = connection error: desc = "transport: Error while dialing dial tcp: lookup badHostNamE: no such host"`},
		{"bad port", []string{"--host=localhost", "--port=12345", "--user=dremio", "--pass=dremio123"},
			`rpc error: code = Unavailable desc = connection error: desc = "transport: Error while dialing dial tcp 127.0.0.1:12345: connectex: No connection could be made because the target machine actively refused it."`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if os.Getenv("TEST_FOO") == "1" {
				os.Args = append([]string{os.Args[0]}, tt.argList...)
				log.SetFlags(0)
				main()
				return
			}

			cmd := exec.Command(os.Args[0], "-test.run="+t.Name())
			cmd.Env = append(os.Environ(), "TEST_FOO=1")
			stdout, _ := cmd.StderrPipe()
			require.NoError(t, cmd.Start())

			gotBytes, _ := ioutil.ReadAll(stdout)
			err := cmd.Wait()
			require.Error(t, err)

			got := strings.TrimSpace(string(gotBytes))
			assert.Equal(t, tt.expected, got)
		})
	}
}
