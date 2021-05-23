package config

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
)

const (
	directory = "./sdbconf/"
	filename  = "sdb.cnf"
)

var defaultConfig = &Config{
	Server: &Server{
		BufferPoolEntryCount: 1000,
		DBFilesDirectory:     "./db/",
		Port:                 5525,
	},
	Client: &Client{},
}

type Config struct {
	Server *Server
	Client *Client
}

type Server struct {
	BufferPoolEntryCount int
	DBFilesDirectory     string
	Port                 int
}

type Client struct{}

func Process() (*Config, error) {
	f, err := os.Open(path.Join(directory, filename))
	if err != nil {
		return nil, err
	}

	defer f.Close()

	c := defaultConfig

	if err := read(f, c); err != nil {
		return nil, err
	}

	return c, nil
}

func read(r io.Reader, c *Config) error {
	scanner := bufio.NewScanner(r)

	var inServerConf bool
	var inClientConf bool
	for scanner.Scan() {
		line := scanner.Text()

		if line == "" {
			continue
		}

		// skip comment
		if strings.HasPrefix(line, "#") {
			continue
		}

		if line == "[server]" {
			inClientConf = false
			inServerConf = true
		}

		if inServerConf {
			readServerConfig(line, c)
		}

		if line == "[client]" {
			inServerConf = false
			inClientConf = true
		}

		if inClientConf {
			readClientConfig(line, c)
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

func readServerConfig(line string, conf *Config) error {
	switch {
	case isLine(line, "buffer_pool_entry_count"):
		v, err := readIntVal(line, "buffer_pool_entry_count")
		if err != nil {
			return err
		}
		conf.Server.BufferPoolEntryCount = v

	case isLine(line, "db_files_directory"):
		conf.Server.DBFilesDirectory = readStringVal(line, "db_files_directory")

	case isLine(line, "port"):
		v, err := readIntVal(line, "port")
		if err != nil {
			return err
		}
		conf.Server.Port = v
	}

	return nil
}

func readClientConfig(line string, conf *Config) error {
	return nil
}

func isLine(line, target string) bool {
	return strings.HasPrefix(line, fmt.Sprintf("%s = ", target))
}

func readIntVal(line, target string) (int, error) {
	v := strings.TrimPrefix(line, fmt.Sprintf("%s = ", target))
	iv, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, err
	}

	return int(iv), nil
}

func readStringVal(line, target string) string {
	return strings.TrimPrefix(line, fmt.Sprintf("%s = ", target))
}
