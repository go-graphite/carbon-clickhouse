package uploader

import (
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/helper/config"
	"github.com/lomik/carbon-clickhouse/helper/tags"
	"github.com/msaf1980/go-stringutils"
	"github.com/pierrec/lz4"
)

type point struct {
	path    string
	value   float64
	time    uint32
	date    uint16
	version uint32
}

func buildCacheMap(points []point, tagged bool) map[string]bool {
	cacheMap := make(map[string]bool)

	for _, point := range points {
		tagDelim := strings.IndexByte(point.path, '?')
		if (tagged && tagDelim == -1) || (!tagged && tagDelim > -1) {
			continue
		}
		cacheMap[strconv.FormatUint(uint64(point.date), 10)+":"+point.path] = true
	}

	return cacheMap
}

func generateMetrics() []point {
	points := make([]point, 0, 4096)

	now := uint32(1559465760)

	hosts := []string{
		"dc1-8d967d8d5-hjxnn",
		"ad2-adf67d8d5-hjkdk1",
	}

	v := float64(0.0)
	for _, host := range hosts {
		for _, name := range []string{
			"blocked",
			"zombies",
			"stopped",
		} {
			pathPlain := "telegraph." + host + ".processes." + name
			pathTagged, _ := tags.Graphite(tags.DisabledTagConfig(), name+"?app=telegraph&host="+host+"&scope=processes")
			date := RowBinary.TimestampToDays(now)

			points = append(points, point{
				path:    pathPlain,
				value:   v,
				time:    now,
				date:    date,
				version: now,
			})
			points = append(points, point{
				path:    pathTagged,
				value:   v,
				time:    now,
				date:    date,
				version: now,
			})
			v += 0.01
		}
	}

	return points
}

func generateMetricsLarge() []point {
	points := make([]point, 0, 4096)

	now := uint32(1559465760)

	hosts := []string{
		"dc1-8d967d8d5-hjxnn",
		"ad2-adf67d8d5-hjkdk1",
		"bt3-9df67d8d3-adkd",
		"9df67d8d3-alkuaef",
		"dc1-8d967d8d5-jis",
		"ad2-8d967d8d5-pis",
		"bt3-9df67d8d3-pkda",
	}

	v := float64(0.0)
	for _, host := range hosts {
		for _, name := range []string{
			"blocked",
			"zombies",
			"stopped",
			"total_threads",
			"sleeping",
			"total",
			"paging",
			"running",
			"dead",
			"unknown",
			"idle",
		} {
			pathPlain := "telegraph." + host + ".processes." + name
			pathTagged, _ := tags.Graphite(tags.DisabledTagConfig(), name+"?app=telegraph&host="+host+"&scope=processes")
			date := RowBinary.TimestampToDays(now)

			points = append(points, point{
				path:    pathPlain,
				value:   v,
				time:    now,
				date:    date,
				version: now,
			})
			points = append(points, point{
				path:    pathTagged,
				value:   v,
				time:    now,
				date:    date,
				version: now,
			})
			v += 0.01
		}
	}

	v = float64(0.0)
	for _, host := range hosts {
		for _, cpu := range []string{
			"cpu-total",
			"cpu0",
			"cpu1",
			"cpu2",
			"cpu3",
		} {
			for _, name := range []string{
				"usage_system",
				"usage_nice",
				"usage_user",
				"usage_steal",
				"usage_guest_nice",
				"usage_iowait",
				"usage_softirq",
				"usage_guest",
				"usage_irq",
				"usage_idle",
			} {
				pathPlain := "telegraph." + host + "." + cpu + ".cpu." + name
				pathTagged, _ := tags.Graphite(tags.DisabledTagConfig(), name+"?app=telegraph&host="+host+"&scope=cpu&cpu="+cpu)
				date := RowBinary.TimestampToDays(now)

				points = append(points, point{
					path:    pathPlain,
					value:   v,
					time:    now,
					date:    date,
					version: now,
				})
				points = append(points, point{
					path:    pathTagged,
					value:   v,
					time:    now,
					date:    date,
					version: now,
				})
				v += 0.01
			}
		}
	}

	v = float64(0.0)
	for _, host := range hosts {
		for _, dev := range []string{
			"loop1",
			"sda1",
			"sda2",
			"sda3",
			"sda21",
		} {
			for _, name := range []string{
				"read_bytes",
				"io_time",
				"weighted_io_time",
				"read_time",
				"write_time",
				"reads",
				"writes",
				"write_bytes",
				"iops_in_progress",
			} {
				pathPlain := "telegraph." + host + "." + dev + ".diskio." + name
				pathTagged, _ := tags.Graphite(tags.DisabledTagConfig(), name+"?app=telegraph&host="+host+"&scope=diskio&dev="+dev)
				date := RowBinary.TimestampToDays(now)

				points = append(points, point{
					path:    pathPlain,
					value:   v,
					time:    now,
					date:    date,
					version: now,
				})
				points = append(points, point{
					path:    pathTagged,
					value:   v,
					time:    now,
					date:    date,
					version: now,
				})
				v += 0.01
			}
		}
	}

	v = float64(0.0)
	for _, host := range hosts {
		for _, name := range []string{
			"in",
			"out",
			"used",
			"free",
			"used_percent",
			"total",
		} {
			pathPlain := "telegraph." + host + ".swap." + name
			pathTagged, _ := tags.Graphite(tags.DisabledTagConfig(), name+"?app=telegraph&host="+host+"&scope=swap")
			date := RowBinary.TimestampToDays(now)

			points = append(points, point{
				path:    pathPlain,
				value:   v,
				time:    now,
				date:    date,
				version: now,
			})
			points = append(points, point{
				path:    pathTagged,
				value:   v,
				time:    now,
				date:    date,
				version: now,
			})
			v += 0.01
		}
	}

	v = float64(0.0)
	for _, host := range hosts {
		for _, name := range []string{
			"huge_pages_total",
			"low_total",
			"write_back_tmp",
			"low_free",
			"total",
			"inactive",
			"vmalloc_used",
			"mapped",
			"cached",
			"huge_pages_free",
			"vmalloc_total",
			"free",
			"active",
			"committed_as",
			"high_free",
			"page_tables",
			"swap_cached",
			"available_percent",
			"used",
			"swap_total",
			"vmalloc_chunk",
			"wired",
			"shared",
			"swap_free",
			"write_back",
			"huge_page_size",
			"buffered",
			"commit_limit",
			"slab",
			"available",
			"dirty",
			"high_total",
			"used_percent",
		} {
			pathPlain := "telegraph." + host + ".mem." + name
			pathTagged, _ := tags.Graphite(tags.DisabledTagConfig(), name+"?app=telegraph&host="+host+"&scope=mem")
			date := RowBinary.TimestampToDays(now)

			points = append(points, point{
				path:    pathPlain,
				value:   v,
				time:    now,
				date:    date,
				version: now,
			})
			points = append(points, point{
				path:    pathTagged,
				value:   v,
				time:    now,
				date:    date,
				version: now,
			})
			v += 0.01
		}
	}

	v = float64(0.0)
	for _, host := range hosts {
		for _, name := range []string{
			"boot_time",
			"processes_forked",
			"entropy_avail",
			"interrupts",
			"context_switches",
		} {
			pathPlain := "telegraph." + host + ".kernel." + name
			pathTagged, _ := tags.Graphite(tags.DisabledTagConfig(), name+"?app=telegraph&host="+host+"&scope=kernel")
			date := RowBinary.TimestampToDays(now)

			points = append(points, point{
				path:    pathPlain,
				value:   v,
				time:    now,
				date:    date,
				version: now,
			})
			points = append(points, point{
				path:    pathTagged,
				value:   v,
				time:    now,
				date:    date,
				version: now,
			})
			v += 0.01
		}
	}

	v = float64(0.0)
	for _, host := range hosts {
		for _, fs := range []string{
			"-mnt-",
			"-",
			"-var",
			"-home",
		} {
			for _, name := range []string{
				"free",
				"inodes_free",
				"inodes_total",
				"inodes_used",
				"total",
				"used",
				"used_percent",
			} {
				pathPlain := "telegraph." + host + "." + fs + ".disk." + name
				pathTagged, _ := tags.Graphite(tags.DisabledTagConfig(), name+"?app=telegraph&host="+host+"&scope=disk&fs="+fs)
				date := RowBinary.TimestampToDays(now)

				points = append(points, point{
					path:    pathPlain,
					value:   v,
					time:    now,
					date:    date,
					version: now,
				})
				points = append(points, point{
					path:    pathTagged,
					value:   v,
					time:    now,
					date:    date,
					version: now,
				})
				v += 0.01
			}
		}
	}

	// some k8s metrics
	k8sEnvironments := []string{"staging", "production", "load_staging", "dev"}
	k8sClusters := []string{"production-cl1", "staging-cl1", "dev-cl1", "private-cl1"}
	k8sDevices := []string{"asf56778", "bFhf5679", "7asf5678", "Afaf56_79", "AsfKj6_764AL"}
	k8sJobs := []string{"kubernetes-nodes-cadvisor", "kubernetes-service-endpoints", "kubernetes-pods"}
	k8sNamespaces := []string{"backend", "frontend", "mysql", "postgres"}
	k8sNames := []string{"cpu.load_avg", "cpu.load_avg10", "cpu.user", "cpu.sys", "cpu.wait", "cpu.idle", "mem.use", "mem.free", "mem.buf", "latency"}

	v = float64(0.0)
	for _, host := range hosts {
		for _, cluster := range k8sClusters {
			for _, namespace := range k8sNamespaces {
				for _, job := range k8sJobs {
					for _, dev := range k8sDevices {
						for _, env := range k8sEnvironments {
							for _, name := range k8sNames {

								pathPlain := cluster + "." +
									env + "." +
									host + "." +
									job + "." +
									dev + "." +
									namespace + "." + name

								pathTagged, _ := tags.Graphite(tags.DisabledTagConfig(), name+
									"?cluster="+cluster+
									"&device="+dev+
									"&environment="+env+
									"&instance="+host+
									"&job="+job+
									"&kubernetes_namespace="+namespace)

								date := RowBinary.TimestampToDays(now)

								points = append(points, point{
									path:    pathPlain,
									value:   v,
									time:    now,
									date:    date,
									version: now,
								})
								points = append(points, point{
									path:    pathTagged,
									value:   v,
									time:    now,
									date:    date,
									version: now,
								})
								v += 0.01
							}
						}
					}
				}
			}
		}
	}

	return points
}

func writeFile(points []point, compress config.CompAlgo, compLevel int) (string, error) {
	file, err := os.CreateTemp("", "rowbinary-")
	if err != nil {
		return "", err
	}

	defer file.Close()

	var wr io.Writer
	switch compress {
	case config.CompAlgoNone:
		wr = file
	case config.CompAlgoLZ4:
		lz4w := lz4.NewWriter(file)
		lz4w.CompressionLevel = compLevel
		lz4w.BlockMaxSize = 4 << 20
		wr = lz4w
	}

	var wb RowBinary.WriteBuffer
	for _, point := range points {
		wb.WriteGraphitePoint(stringutils.UnsafeStringBytes(&point.path), point.value, point.version, point.version)
		if _, err = wr.Write(wb.Bytes()); err != nil {
			os.Remove(file.Name())
			return "", err
		}
		wb.Reset()
	}

	return file.Name(), err
}
