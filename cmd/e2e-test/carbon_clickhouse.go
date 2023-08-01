package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"syscall"
	"text/template"
)

type CarbonClickhouse struct {
	Binary    string `toml:"binary"`
	ConfigTpl string `toml:"template"`
	TestDir   string `toml:"-"`

	storeDir   string    `toml:"-"`
	configFile string    `toml:"-"`
	address    string    `toml:"-"`
	cmd        *exec.Cmd `toml:"-"`
}

func (c *CarbonClickhouse) Start(clickhouseURL string, clickhouseTLSURL string) error {
	if c.cmd != nil {
		return fmt.Errorf("carbon-clickhouse already started")
	}

	if len(c.Binary) == 0 {
		c.Binary = "./carbon-clickhouse"
	}
	if len(c.ConfigTpl) == 0 {
		return fmt.Errorf("carbon-clickhouse config template not set")
	}

	var err error
	c.storeDir, err = ioutil.TempDir("", "carbon-clickhouse")
	if err != nil {
		return err
	}

	c.address, err = getFreeTCPPort("")
	if err != nil {
		c.Cleanup()
		return err
	}

	name := filepath.Base(c.ConfigTpl)
	tmpl, err := template.New(name).ParseFiles(c.ConfigTpl)
	if err != nil {
		c.Cleanup()
		return err
	}
	param := struct {
		CLICKHOUSE_URL     string
		CLICKHOUSE_TLS_URL string
		CCH_STORE_DIR      string
		CCH_ADDR           string
		TEST_DIR           string
	}{
		CLICKHOUSE_URL:     clickhouseURL,
		CLICKHOUSE_TLS_URL: clickhouseTLSURL,
		CCH_STORE_DIR:      c.storeDir,
		CCH_ADDR:           c.address,
		TEST_DIR:           c.TestDir,
	}

	c.configFile = path.Join(c.storeDir, "carbon-clickhouse.conf")
	f, err := os.OpenFile(c.configFile, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		c.Cleanup()
		return err
	}
	err = tmpl.ExecuteTemplate(f, name, param)
	if err != nil {
		c.Cleanup()
		return err
	}

	c.cmd = exec.Command(c.Binary, "-config", c.configFile)
	c.cmd.Stdout = os.Stdout
	c.cmd.Stderr = os.Stderr
	//c.cmd.Env = append(c.cmd.Env, "TZ=UTC")
	err = c.cmd.Start()
	if err != nil {
		c.Cleanup()
		return err
	}

	return nil
}

func (c *CarbonClickhouse) Stop() error {
	if c.cmd == nil {
		return nil
	}
	var err error
	if err = c.cmd.Process.Kill(); err == nil {
		if err = c.cmd.Wait(); err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
					ec := status.ExitStatus()
					if ec == 0 || ec == -1 {
						return nil
					}
				}
			}
		}
	}
	return err
}

func (c *CarbonClickhouse) Cleanup() {
	if len(c.storeDir) > 0 {
		os.RemoveAll(c.storeDir)
		c.storeDir = ""
		c.cmd = nil
	}
}

func (c *CarbonClickhouse) Address() string {
	return c.address
}
