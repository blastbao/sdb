package cli

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/dty1er/sdb/server"
)

type Runner interface {
	Name() string
	Init([]string) error
	Run() error
}

func Run(args []string) error {
	cmds := []Runner{
		NewDebugCommand(),
		NewServerCommand(),
	}

	if len(os.Args) < 2 {
		return runCli()
	}

	subcommand := args[0]

	for _, cmd := range cmds {
		if cmd.Name() == subcommand {
			cmd.Init(args[1:])
			return cmd.Run()
		}
	}

	return runCli()
}

// TODO: handle up/down arrow, history feature, ctrl-c/d handling, etc.
func runCli() error {
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("sdb> ")
		query, err := reader.ReadString('\n')
		if err != nil {
			return err
		}
		query = strings.TrimRight(query, "\n")

		for !strings.HasSuffix(query, ";") {
			fmt.Print("> ")
			t, err := reader.ReadString('\n')
			if err != nil {
				return err
			}
			t = strings.TrimRight(t, "\n")

			query += " " + t
		}

		resp, err := ExecQuery(query)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			continue
		}

		if resp.Code != "OK" {
			fmt.Fprintf(os.Stdout, "execution failed: %s\n", resp.Error.Message)
			continue
		}

		fmt.Fprintf(os.Stdout, "%+v\n", resp.RS)
	}
}

func ExecQuery(query string) (*server.Response, error) {
	r := server.Request{Query: query}
	reqB, err := json.Marshal(&r)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", "http://localhost:5525/execute", bytes.NewBuffer(reqB))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	var sdbResp server.Response
	if err := json.NewDecoder(resp.Body).Decode(&sdbResp); err != nil {
		return nil, err
	}

	return &sdbResp, nil
}
