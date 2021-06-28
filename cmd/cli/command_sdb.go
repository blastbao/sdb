package cli

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/dty1er/sdb/engine"
	"github.com/dty1er/sdb/server"
	"github.com/dty1er/sdb/tablewriter"
)

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

		// FUTURE WORK: change url based on the command line arguments
		resp, err := ExecQuery("http://localhost:5525", query)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			continue
		}

		if resp.Code != "OK" {
			fmt.Fprintf(os.Stdout, "execution failed: %s\n", resp.Error.Message)
			continue
		}

		tw := tablewriter.New(os.Stdout)
		tw.SetHeader(resp.RS.Columns)
		for _, val := range resp.RS.Values {
			vals := []string{}
			for _, v := range val.Data {
				fmt.Println(v)
				switch v.Typ {
				case engine.Bool:
					vals = append(vals, fmt.Sprintf("%v", v.BoolVal))
				case engine.Int64:
					vals = append(vals, fmt.Sprintf("%v", v.Int64Val))
				case engine.Float64:
					vals = append(vals, fmt.Sprintf("%v", v.Float64Val))
				case engine.Bytes:
					vals = append(vals, fmt.Sprintf("%v", v.BytesVal))
				case engine.String:
					vals = append(vals, fmt.Sprintf("%v", v.StringVal))
				case engine.Timestamp:
					fmt.Println(v.TimestampVal)
					vals = append(vals, fmt.Sprintf("%v", time.Unix(v.TimestampVal, 0).Format("2006-01-02 15:04:05")))
				}
			}
			tw.Append(vals)
		}
		tw.Render()
		fmt.Fprintf(os.Stdout, "Count: %d\n", resp.RS.Count)
	}
}

func ExecQuery(address, query string) (*server.Response, error) {
	r := server.Request{Query: query}
	reqB, err := json.Marshal(&r)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/execute", address), bytes.NewBuffer(reqB))
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

	fmt.Printf("%#v\n", sdbResp)

	return &sdbResp, nil
}
