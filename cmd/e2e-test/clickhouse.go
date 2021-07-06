package main

import (
	"fmt"
	"os/exec"
	"strconv"

	"github.com/phayes/freeport"
)

type Clickhouse struct {
	Version string `toml:"version"`
	Dir     string `toml:"dir"`

	Docker      string `toml:"docker"`
	DockerImage string `toml:"image"`

	address   string `toml:"-"`
	container string `toml:"-"`
}

func (c *Clickhouse) Start() (error, string) {
	if len(c.Version) == 0 {
		return fmt.Errorf("version not set"), ""
	}
	if len(c.Dir) == 0 {
		return fmt.Errorf("dir not set"), ""
	}
	if len(c.Docker) == 0 {
		c.Docker = "docker"
	}
	if len(c.DockerImage) == 0 {
		c.DockerImage = "yandex/clickhouse-server"
	}
	port, err := freeport.GetFreePort()
	if err != nil {
		return err, ""
	}

	c.address = "127.0.0.1:" + strconv.Itoa(port)
	c.container = "carbon-clickhouse-clickhouse-server-test"

	chStart := []string{"run", "-d",
		"--name", c.container,
		"--ulimit", "nofile=262144:262144",
		"-p", c.address + ":8123",
		//"-e", "TZ=UTC",
		"-v", c.Dir + "/config.xml:/etc/clickhouse-server/config.xml",
		"-v", c.Dir + "/users.xml:/etc/clickhouse-server/users.xml",
		"-v", c.Dir + "/rollup.xml:/etc/clickhouse-server/config.d/rollup.xml",
		"-v", c.Dir + "/init.sql:/docker-entrypoint-initdb.d/init.sql",
		c.DockerImage + ":" + c.Version,
	}

	cmd := exec.Command(c.Docker, chStart...)
	out, err := cmd.CombinedOutput()

	return err, string(out)
}

func (c *Clickhouse) Stop(delete bool) (error, string) {
	if len(c.container) == 0 {
		return nil, ""
	}

	chStop := []string{"stop", c.container}

	cmd := exec.Command(c.Docker, chStop...)
	out, err := cmd.CombinedOutput()

	if err == nil && delete {
		return c.Delete()
	}
	return err, string(out)
}

func (c *Clickhouse) Delete() (error, string) {
	if len(c.container) == 0 {
		return nil, ""
	}

	chDel := []string{"rm", c.container}

	cmd := exec.Command(c.Docker, chDel...)
	out, err := cmd.CombinedOutput()

	return err, string(out)
}

func (c *Clickhouse) Address() string {
	return c.address
}

func (c *Clickhouse) Container() string {
	return c.container
}
