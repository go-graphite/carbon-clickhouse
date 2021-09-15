package uploader

import (
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/helper/escape"
	"github.com/lomik/zapwriter"
	"github.com/msaf1980/go-stringutils"
	"github.com/stretchr/testify/assert"
)

func TestUrlParse(t *testing.T) {
	assert := assert.New(t)

	// make metric name as receiver
	metric := escape.Path("instance:cpu_utilization:ratio_avg") +
		"?" + escape.Query("dc") + "=" + escape.Query("qwe") +
		"&" + escape.Query("fqdn") + "=" + escape.Query("asd") +
		"&" + escape.Query("instance") + "=" + escape.Query("10.33.10.10:9100") +
		"&" + escape.Query("job") + "=" + escape.Query("node")

	assert.Equal("instance:cpu_utilization:ratio_avg?dc=qwe&fqdn=asd&instance=10.33.10.10%3A9100&job=node", metric)

	// original url.Parse
	m, err := url.Parse(metric)
	assert.NotNil(m)
	assert.NoError(err)
	assert.Equal("", m.Path)

	// from tagged uploader
	m, err = urlParse(metric)
	assert.NotNil(m)
	assert.NoError(err)
	assert.Equal("instance:cpu_utilization:ratio_avg", m.Path)
}

func BenchmarkKeySprintf(b *testing.B) {
	path := "test.path"

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = fmt.Sprintf("%d:%s", 1285, path)
	}
}

func BenchmarkKeyConcat(b *testing.B) {
	path := "test.path"
	var unum uint16 = 1245

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = strconv.Itoa(int(unum)) + ":" + path
	}
}

func TestTagged_parseName_Overflow(t *testing.T) {
	var tag1 []string
	wb := RowBinary.GetWriteBuffer()
	tagsBuf := RowBinary.GetWriteBuffer()
	defer wb.Release()
	defer tagsBuf.Release()

	logger := zapwriter.Logger("upload")
	base := &Base{
		queue:   make(chan string, 1024),
		inQueue: make(map[string]bool),
		logger:  logger,
		config:  &Config{TableName: "test"},
	}
	var sb strings.Builder
	sb.WriteString("very_long_name_field1.very_long_name_field2.very_long_name_field3.very_long_name_field4?")
	for i := 0; i < 100; i++ {
		if i > 0 {
			sb.WriteString("&")
		}
		sb.WriteString(fmt.Sprintf("very_long_tag%d=very_long_value%d", i, i))
	}
	u := NewTagged(base)
	err := u.parseName(sb.String(), 10, 1024, tag1, wb, tagsBuf)
	assert.Equal(t, errBufOverflow, err)
}

type taggedRecord struct {
	path    string
	name    string
	tags    []string
	days    uint16
	version uint32
}

func (p *taggedRecord) Dump() string {
	var sb stringutils.Builder
	sb.WriteString("  path='")
	sb.WriteString(p.path)
	sb.WriteString("',\n  name='")
	sb.WriteString(p.name)
	sb.WriteString("',\n  days=")
	sb.WriteUint(uint64(p.days), 10)
	sb.WriteString(",\n  version=")
	sb.WriteUint(uint64(p.version), 10)
	sb.WriteString(",\n  tags=")
	sb.WriteString(strings.Join(p.tags, " , "))

	return sb.String()
}

func parseTags(b []byte) ([]*taggedRecord, error) {
	var err error
	var result []*taggedRecord

	d := newDecoder(b)

	for {
		if d.pos == len(d.b) {
			// EOF
			break
		}
		tagged := &taggedRecord{}
		tagged.days = d.GetUint16()
		tagged.name = d.GetString()
		tagged.path = d.GetString()
		tagged.tags = d.GetStrings()
		tagged.version = d.GetUint32()

		result = append(result, tagged)
	}

	return result, err
}

func TestTagged_parseName(t *testing.T) {
	// locate reusable buffers
	tag1 := make([]string, 0)
	wb := RowBinary.GetWriteBuffer()
	tagsBuf := RowBinary.GetWriteBuffer()
	defer wb.Release()
	defer tagsBuf.Release()

	logger := zapwriter.Logger("upload")

	// type fields struct {
	// 	cached         *cached
	// 	ignoredMetrics map[string]bool
	// }

	tests := []struct {
		name           string
		days           uint16
		version        uint32
		ignoredMetrics []string
		wantErr        bool
		wantResult     []*taggedRecord //tags set in wantTags for avoid copy-past, days and version checked from test struct
		wantTags       []string
	}{
		{
			name:    "instance:cpu_utilization:ratio_avg?dc=qwe&fqdn=asd&instance=10.33.10.10_9100&job=node",
			days:    1024,
			version: 2,
			wantErr: false,
			wantTags: []string{
				"__name__=instance:cpu_utilization:ratio_avg",
				"dc=qwe",
				"fqdn=asd",
				"instance=10.33.10.10_9100",
				"job=node",
			},
			wantResult: []*taggedRecord{
				{
					path: "instance:cpu_utilization:ratio_avg?dc=qwe&fqdn=asd&instance=10.33.10.10_9100&job=node",
					name: "__name__=instance:cpu_utilization:ratio_avg",
				},
				{
					path: "instance:cpu_utilization:ratio_avg?dc=qwe&fqdn=asd&instance=10.33.10.10_9100&job=node",
					name: "dc=qwe",
				},
				{
					path: "instance:cpu_utilization:ratio_avg?dc=qwe&fqdn=asd&instance=10.33.10.10_9100&job=node",
					name: "fqdn=asd",
				},
				{
					path: "instance:cpu_utilization:ratio_avg?dc=qwe&fqdn=asd&instance=10.33.10.10_9100&job=node",
					name: "instance=10.33.10.10_9100",
				},
				{
					path: "instance:cpu_utilization:ratio_avg?dc=qwe&fqdn=asd&instance=10.33.10.10_9100&job=node",
					name: "job=node",
				},
			},
		},
		{
			name:           "ignored_ratio_avg?dc=qwe&fqdn=asd&instance=10.33.10.10_9100&job=node",
			days:           1024,
			version:        2,
			ignoredMetrics: []string{"ignored_ratio_avg"},
			wantErr:        false,
			wantTags: []string{
				"__name__=ignored_ratio_avg",
				"dc=qwe",
				"fqdn=asd",
				"instance=10.33.10.10_9100",
				"job=node",
			},
			wantResult: []*taggedRecord{
				{
					path: "ignored_ratio_avg?dc=qwe&fqdn=asd&instance=10.33.10.10_9100&job=node",
					name: "__name__=ignored_ratio_avg",
				},
			},
		},
		{
			name:           "ignored_all_ratio_avg?dc=qwe&fqdn=asd&instance=10.33.10.10_9100&job=node",
			days:           1024,
			version:        2,
			ignoredMetrics: []string{"*"},
			wantErr:        false,
			wantTags: []string{
				"__name__=ignored_all_ratio_avg",
				"dc=qwe",
				"fqdn=asd",
				"instance=10.33.10.10_9100",
				"job=node",
			},
			wantResult: []*taggedRecord{
				{
					path: "ignored_all_ratio_avg?dc=qwe&fqdn=asd&instance=10.33.10.10_9100&job=node",
					name: "__name__=ignored_all_ratio_avg",
				},
			},
		},
		{
			name:    "k8s.production-cl1.nginx_ingress_controller_response_size_bucket?app_kubernetes_io_component=controller&app_kubernetes_io_instance=ingress-nginx&app_kubernetes_io_managed_by=Helm&app_kubernetes_io_name=ingress-nginx&app_kubernetes_io_version=0_32_0&controller_class=nginx&controller_namespace=ingress-nginx&controller_pod=ingress-nginx-controller-d2ppr&helm_sh_chart=ingress-nginx-2_3_0&host=vm1_test_int&ingress=web-ingress&instance=192_168_0.10&job=kubernetes-service-endpoints&kubernetes_name=ingress-nginx-controller-metrics&kubernetes_namespace=ingress-nginx&kubernetes_node=k8s-n03&le=10&method=GET&namespace=web-app&path=_&service=web-app&status=500",
			days:    1024,
			version: 2,
			wantErr: false,
			wantTags: []string{
				"__name__=k8s.production-cl1.nginx_ingress_controller_response_size_bucket",
				"app_kubernetes_io_component=controller",
				"app_kubernetes_io_instance=ingress-nginx",
				"app_kubernetes_io_managed_by=Helm",
				"app_kubernetes_io_name=ingress-nginx",
				"app_kubernetes_io_version=0_32_0",
				"controller_class=nginx",
				"controller_namespace=ingress-nginx",
				"controller_pod=ingress-nginx-controller-d2ppr",
				"helm_sh_chart=ingress-nginx-2_3_0",
				"host=vm1_test_int",
				"ingress=web-ingress",
				"instance=192_168_0.10",
				"job=kubernetes-service-endpoints",
				"kubernetes_name=ingress-nginx-controller-metrics",
				"kubernetes_namespace=ingress-nginx",
				"kubernetes_node=k8s-n03",
				"le=10",
				"method=GET",
				"namespace=web-app",
				"path=_",
				"service=web-app",
				"status=500",
			},
			wantResult: []*taggedRecord{
				{
					path: "k8s.production-cl1.nginx_ingress_controller_response_size_bucket?app_kubernetes_io_component=controller&app_kubernetes_io_instance=ingress-nginx&app_kubernetes_io_managed_by=Helm&app_kubernetes_io_name=ingress-nginx&app_kubernetes_io_version=0_32_0&controller_class=nginx&controller_namespace=ingress-nginx&controller_pod=ingress-nginx-controller-d2ppr&helm_sh_chart=ingress-nginx-2_3_0&host=vm1_test_int&ingress=web-ingress&instance=192_168_0.10&job=kubernetes-service-endpoints&kubernetes_name=ingress-nginx-controller-metrics&kubernetes_namespace=ingress-nginx&kubernetes_node=k8s-n03&le=10&method=GET&namespace=web-app&path=_&service=web-app&status=500",
					name: "__name__=k8s.production-cl1.nginx_ingress_controller_response_size_bucket",
				},
				{
					path: "k8s.production-cl1.nginx_ingress_controller_response_size_bucket?app_kubernetes_io_component=controller&app_kubernetes_io_instance=ingress-nginx&app_kubernetes_io_managed_by=Helm&app_kubernetes_io_name=ingress-nginx&app_kubernetes_io_version=0_32_0&controller_class=nginx&controller_namespace=ingress-nginx&controller_pod=ingress-nginx-controller-d2ppr&helm_sh_chart=ingress-nginx-2_3_0&host=vm1_test_int&ingress=web-ingress&instance=192_168_0.10&job=kubernetes-service-endpoints&kubernetes_name=ingress-nginx-controller-metrics&kubernetes_namespace=ingress-nginx&kubernetes_node=k8s-n03&le=10&method=GET&namespace=web-app&path=_&service=web-app&status=500",
					name: "app_kubernetes_io_component=controller",
				},
				{
					path: "k8s.production-cl1.nginx_ingress_controller_response_size_bucket?app_kubernetes_io_component=controller&app_kubernetes_io_instance=ingress-nginx&app_kubernetes_io_managed_by=Helm&app_kubernetes_io_name=ingress-nginx&app_kubernetes_io_version=0_32_0&controller_class=nginx&controller_namespace=ingress-nginx&controller_pod=ingress-nginx-controller-d2ppr&helm_sh_chart=ingress-nginx-2_3_0&host=vm1_test_int&ingress=web-ingress&instance=192_168_0.10&job=kubernetes-service-endpoints&kubernetes_name=ingress-nginx-controller-metrics&kubernetes_namespace=ingress-nginx&kubernetes_node=k8s-n03&le=10&method=GET&namespace=web-app&path=_&service=web-app&status=500",
					name: "app_kubernetes_io_instance=ingress-nginx",
				},
				{
					path: "k8s.production-cl1.nginx_ingress_controller_response_size_bucket?app_kubernetes_io_component=controller&app_kubernetes_io_instance=ingress-nginx&app_kubernetes_io_managed_by=Helm&app_kubernetes_io_name=ingress-nginx&app_kubernetes_io_version=0_32_0&controller_class=nginx&controller_namespace=ingress-nginx&controller_pod=ingress-nginx-controller-d2ppr&helm_sh_chart=ingress-nginx-2_3_0&host=vm1_test_int&ingress=web-ingress&instance=192_168_0.10&job=kubernetes-service-endpoints&kubernetes_name=ingress-nginx-controller-metrics&kubernetes_namespace=ingress-nginx&kubernetes_node=k8s-n03&le=10&method=GET&namespace=web-app&path=_&service=web-app&status=500",
					name: "app_kubernetes_io_managed_by=Helm",
				},
				{
					path: "k8s.production-cl1.nginx_ingress_controller_response_size_bucket?app_kubernetes_io_component=controller&app_kubernetes_io_instance=ingress-nginx&app_kubernetes_io_managed_by=Helm&app_kubernetes_io_name=ingress-nginx&app_kubernetes_io_version=0_32_0&controller_class=nginx&controller_namespace=ingress-nginx&controller_pod=ingress-nginx-controller-d2ppr&helm_sh_chart=ingress-nginx-2_3_0&host=vm1_test_int&ingress=web-ingress&instance=192_168_0.10&job=kubernetes-service-endpoints&kubernetes_name=ingress-nginx-controller-metrics&kubernetes_namespace=ingress-nginx&kubernetes_node=k8s-n03&le=10&method=GET&namespace=web-app&path=_&service=web-app&status=500",
					name: "app_kubernetes_io_name=ingress-nginx",
				},
				{
					path: "k8s.production-cl1.nginx_ingress_controller_response_size_bucket?app_kubernetes_io_component=controller&app_kubernetes_io_instance=ingress-nginx&app_kubernetes_io_managed_by=Helm&app_kubernetes_io_name=ingress-nginx&app_kubernetes_io_version=0_32_0&controller_class=nginx&controller_namespace=ingress-nginx&controller_pod=ingress-nginx-controller-d2ppr&helm_sh_chart=ingress-nginx-2_3_0&host=vm1_test_int&ingress=web-ingress&instance=192_168_0.10&job=kubernetes-service-endpoints&kubernetes_name=ingress-nginx-controller-metrics&kubernetes_namespace=ingress-nginx&kubernetes_node=k8s-n03&le=10&method=GET&namespace=web-app&path=_&service=web-app&status=500",
					name: "app_kubernetes_io_version=0_32_0",
				},
				{
					path: "k8s.production-cl1.nginx_ingress_controller_response_size_bucket?app_kubernetes_io_component=controller&app_kubernetes_io_instance=ingress-nginx&app_kubernetes_io_managed_by=Helm&app_kubernetes_io_name=ingress-nginx&app_kubernetes_io_version=0_32_0&controller_class=nginx&controller_namespace=ingress-nginx&controller_pod=ingress-nginx-controller-d2ppr&helm_sh_chart=ingress-nginx-2_3_0&host=vm1_test_int&ingress=web-ingress&instance=192_168_0.10&job=kubernetes-service-endpoints&kubernetes_name=ingress-nginx-controller-metrics&kubernetes_namespace=ingress-nginx&kubernetes_node=k8s-n03&le=10&method=GET&namespace=web-app&path=_&service=web-app&status=500",
					name: "controller_class=nginx",
				},
				{
					path: "k8s.production-cl1.nginx_ingress_controller_response_size_bucket?app_kubernetes_io_component=controller&app_kubernetes_io_instance=ingress-nginx&app_kubernetes_io_managed_by=Helm&app_kubernetes_io_name=ingress-nginx&app_kubernetes_io_version=0_32_0&controller_class=nginx&controller_namespace=ingress-nginx&controller_pod=ingress-nginx-controller-d2ppr&helm_sh_chart=ingress-nginx-2_3_0&host=vm1_test_int&ingress=web-ingress&instance=192_168_0.10&job=kubernetes-service-endpoints&kubernetes_name=ingress-nginx-controller-metrics&kubernetes_namespace=ingress-nginx&kubernetes_node=k8s-n03&le=10&method=GET&namespace=web-app&path=_&service=web-app&status=500",
					name: "controller_namespace=ingress-nginx",
				},
				{
					path: "k8s.production-cl1.nginx_ingress_controller_response_size_bucket?app_kubernetes_io_component=controller&app_kubernetes_io_instance=ingress-nginx&app_kubernetes_io_managed_by=Helm&app_kubernetes_io_name=ingress-nginx&app_kubernetes_io_version=0_32_0&controller_class=nginx&controller_namespace=ingress-nginx&controller_pod=ingress-nginx-controller-d2ppr&helm_sh_chart=ingress-nginx-2_3_0&host=vm1_test_int&ingress=web-ingress&instance=192_168_0.10&job=kubernetes-service-endpoints&kubernetes_name=ingress-nginx-controller-metrics&kubernetes_namespace=ingress-nginx&kubernetes_node=k8s-n03&le=10&method=GET&namespace=web-app&path=_&service=web-app&status=500",
					name: "controller_pod=ingress-nginx-controller-d2ppr",
				},
				{
					path: "k8s.production-cl1.nginx_ingress_controller_response_size_bucket?app_kubernetes_io_component=controller&app_kubernetes_io_instance=ingress-nginx&app_kubernetes_io_managed_by=Helm&app_kubernetes_io_name=ingress-nginx&app_kubernetes_io_version=0_32_0&controller_class=nginx&controller_namespace=ingress-nginx&controller_pod=ingress-nginx-controller-d2ppr&helm_sh_chart=ingress-nginx-2_3_0&host=vm1_test_int&ingress=web-ingress&instance=192_168_0.10&job=kubernetes-service-endpoints&kubernetes_name=ingress-nginx-controller-metrics&kubernetes_namespace=ingress-nginx&kubernetes_node=k8s-n03&le=10&method=GET&namespace=web-app&path=_&service=web-app&status=500",
					name: "helm_sh_chart=ingress-nginx-2_3_0",
				},
				{
					path: "k8s.production-cl1.nginx_ingress_controller_response_size_bucket?app_kubernetes_io_component=controller&app_kubernetes_io_instance=ingress-nginx&app_kubernetes_io_managed_by=Helm&app_kubernetes_io_name=ingress-nginx&app_kubernetes_io_version=0_32_0&controller_class=nginx&controller_namespace=ingress-nginx&controller_pod=ingress-nginx-controller-d2ppr&helm_sh_chart=ingress-nginx-2_3_0&host=vm1_test_int&ingress=web-ingress&instance=192_168_0.10&job=kubernetes-service-endpoints&kubernetes_name=ingress-nginx-controller-metrics&kubernetes_namespace=ingress-nginx&kubernetes_node=k8s-n03&le=10&method=GET&namespace=web-app&path=_&service=web-app&status=500",
					name: "host=vm1_test_int",
				},
				{
					path: "k8s.production-cl1.nginx_ingress_controller_response_size_bucket?app_kubernetes_io_component=controller&app_kubernetes_io_instance=ingress-nginx&app_kubernetes_io_managed_by=Helm&app_kubernetes_io_name=ingress-nginx&app_kubernetes_io_version=0_32_0&controller_class=nginx&controller_namespace=ingress-nginx&controller_pod=ingress-nginx-controller-d2ppr&helm_sh_chart=ingress-nginx-2_3_0&host=vm1_test_int&ingress=web-ingress&instance=192_168_0.10&job=kubernetes-service-endpoints&kubernetes_name=ingress-nginx-controller-metrics&kubernetes_namespace=ingress-nginx&kubernetes_node=k8s-n03&le=10&method=GET&namespace=web-app&path=_&service=web-app&status=500",
					name: "ingress=web-ingress",
				},
				{
					path: "k8s.production-cl1.nginx_ingress_controller_response_size_bucket?app_kubernetes_io_component=controller&app_kubernetes_io_instance=ingress-nginx&app_kubernetes_io_managed_by=Helm&app_kubernetes_io_name=ingress-nginx&app_kubernetes_io_version=0_32_0&controller_class=nginx&controller_namespace=ingress-nginx&controller_pod=ingress-nginx-controller-d2ppr&helm_sh_chart=ingress-nginx-2_3_0&host=vm1_test_int&ingress=web-ingress&instance=192_168_0.10&job=kubernetes-service-endpoints&kubernetes_name=ingress-nginx-controller-metrics&kubernetes_namespace=ingress-nginx&kubernetes_node=k8s-n03&le=10&method=GET&namespace=web-app&path=_&service=web-app&status=500",
					name: "instance=192_168_0.10",
				},
				{
					path: "k8s.production-cl1.nginx_ingress_controller_response_size_bucket?app_kubernetes_io_component=controller&app_kubernetes_io_instance=ingress-nginx&app_kubernetes_io_managed_by=Helm&app_kubernetes_io_name=ingress-nginx&app_kubernetes_io_version=0_32_0&controller_class=nginx&controller_namespace=ingress-nginx&controller_pod=ingress-nginx-controller-d2ppr&helm_sh_chart=ingress-nginx-2_3_0&host=vm1_test_int&ingress=web-ingress&instance=192_168_0.10&job=kubernetes-service-endpoints&kubernetes_name=ingress-nginx-controller-metrics&kubernetes_namespace=ingress-nginx&kubernetes_node=k8s-n03&le=10&method=GET&namespace=web-app&path=_&service=web-app&status=500",
					name: "job=kubernetes-service-endpoints",
				},
				{
					path: "k8s.production-cl1.nginx_ingress_controller_response_size_bucket?app_kubernetes_io_component=controller&app_kubernetes_io_instance=ingress-nginx&app_kubernetes_io_managed_by=Helm&app_kubernetes_io_name=ingress-nginx&app_kubernetes_io_version=0_32_0&controller_class=nginx&controller_namespace=ingress-nginx&controller_pod=ingress-nginx-controller-d2ppr&helm_sh_chart=ingress-nginx-2_3_0&host=vm1_test_int&ingress=web-ingress&instance=192_168_0.10&job=kubernetes-service-endpoints&kubernetes_name=ingress-nginx-controller-metrics&kubernetes_namespace=ingress-nginx&kubernetes_node=k8s-n03&le=10&method=GET&namespace=web-app&path=_&service=web-app&status=500",
					name: "kubernetes_name=ingress-nginx-controller-metrics",
				},
				{
					path: "k8s.production-cl1.nginx_ingress_controller_response_size_bucket?app_kubernetes_io_component=controller&app_kubernetes_io_instance=ingress-nginx&app_kubernetes_io_managed_by=Helm&app_kubernetes_io_name=ingress-nginx&app_kubernetes_io_version=0_32_0&controller_class=nginx&controller_namespace=ingress-nginx&controller_pod=ingress-nginx-controller-d2ppr&helm_sh_chart=ingress-nginx-2_3_0&host=vm1_test_int&ingress=web-ingress&instance=192_168_0.10&job=kubernetes-service-endpoints&kubernetes_name=ingress-nginx-controller-metrics&kubernetes_namespace=ingress-nginx&kubernetes_node=k8s-n03&le=10&method=GET&namespace=web-app&path=_&service=web-app&status=500",
					name: "kubernetes_namespace=ingress-nginx",
				},
				{
					path: "k8s.production-cl1.nginx_ingress_controller_response_size_bucket?app_kubernetes_io_component=controller&app_kubernetes_io_instance=ingress-nginx&app_kubernetes_io_managed_by=Helm&app_kubernetes_io_name=ingress-nginx&app_kubernetes_io_version=0_32_0&controller_class=nginx&controller_namespace=ingress-nginx&controller_pod=ingress-nginx-controller-d2ppr&helm_sh_chart=ingress-nginx-2_3_0&host=vm1_test_int&ingress=web-ingress&instance=192_168_0.10&job=kubernetes-service-endpoints&kubernetes_name=ingress-nginx-controller-metrics&kubernetes_namespace=ingress-nginx&kubernetes_node=k8s-n03&le=10&method=GET&namespace=web-app&path=_&service=web-app&status=500",
					name: "kubernetes_node=k8s-n03",
				},
				{
					path: "k8s.production-cl1.nginx_ingress_controller_response_size_bucket?app_kubernetes_io_component=controller&app_kubernetes_io_instance=ingress-nginx&app_kubernetes_io_managed_by=Helm&app_kubernetes_io_name=ingress-nginx&app_kubernetes_io_version=0_32_0&controller_class=nginx&controller_namespace=ingress-nginx&controller_pod=ingress-nginx-controller-d2ppr&helm_sh_chart=ingress-nginx-2_3_0&host=vm1_test_int&ingress=web-ingress&instance=192_168_0.10&job=kubernetes-service-endpoints&kubernetes_name=ingress-nginx-controller-metrics&kubernetes_namespace=ingress-nginx&kubernetes_node=k8s-n03&le=10&method=GET&namespace=web-app&path=_&service=web-app&status=500",
					name: "le=10",
				},
				{
					path: "k8s.production-cl1.nginx_ingress_controller_response_size_bucket?app_kubernetes_io_component=controller&app_kubernetes_io_instance=ingress-nginx&app_kubernetes_io_managed_by=Helm&app_kubernetes_io_name=ingress-nginx&app_kubernetes_io_version=0_32_0&controller_class=nginx&controller_namespace=ingress-nginx&controller_pod=ingress-nginx-controller-d2ppr&helm_sh_chart=ingress-nginx-2_3_0&host=vm1_test_int&ingress=web-ingress&instance=192_168_0.10&job=kubernetes-service-endpoints&kubernetes_name=ingress-nginx-controller-metrics&kubernetes_namespace=ingress-nginx&kubernetes_node=k8s-n03&le=10&method=GET&namespace=web-app&path=_&service=web-app&status=500",
					name: "method=GET",
				},
				{
					path: "k8s.production-cl1.nginx_ingress_controller_response_size_bucket?app_kubernetes_io_component=controller&app_kubernetes_io_instance=ingress-nginx&app_kubernetes_io_managed_by=Helm&app_kubernetes_io_name=ingress-nginx&app_kubernetes_io_version=0_32_0&controller_class=nginx&controller_namespace=ingress-nginx&controller_pod=ingress-nginx-controller-d2ppr&helm_sh_chart=ingress-nginx-2_3_0&host=vm1_test_int&ingress=web-ingress&instance=192_168_0.10&job=kubernetes-service-endpoints&kubernetes_name=ingress-nginx-controller-metrics&kubernetes_namespace=ingress-nginx&kubernetes_node=k8s-n03&le=10&method=GET&namespace=web-app&path=_&service=web-app&status=500",
					name: "namespace=web-app",
				},
				{
					path: "k8s.production-cl1.nginx_ingress_controller_response_size_bucket?app_kubernetes_io_component=controller&app_kubernetes_io_instance=ingress-nginx&app_kubernetes_io_managed_by=Helm&app_kubernetes_io_name=ingress-nginx&app_kubernetes_io_version=0_32_0&controller_class=nginx&controller_namespace=ingress-nginx&controller_pod=ingress-nginx-controller-d2ppr&helm_sh_chart=ingress-nginx-2_3_0&host=vm1_test_int&ingress=web-ingress&instance=192_168_0.10&job=kubernetes-service-endpoints&kubernetes_name=ingress-nginx-controller-metrics&kubernetes_namespace=ingress-nginx&kubernetes_node=k8s-n03&le=10&method=GET&namespace=web-app&path=_&service=web-app&status=500",
					name: "path=_",
				},
				{
					path: "k8s.production-cl1.nginx_ingress_controller_response_size_bucket?app_kubernetes_io_component=controller&app_kubernetes_io_instance=ingress-nginx&app_kubernetes_io_managed_by=Helm&app_kubernetes_io_name=ingress-nginx&app_kubernetes_io_version=0_32_0&controller_class=nginx&controller_namespace=ingress-nginx&controller_pod=ingress-nginx-controller-d2ppr&helm_sh_chart=ingress-nginx-2_3_0&host=vm1_test_int&ingress=web-ingress&instance=192_168_0.10&job=kubernetes-service-endpoints&kubernetes_name=ingress-nginx-controller-metrics&kubernetes_namespace=ingress-nginx&kubernetes_node=k8s-n03&le=10&method=GET&namespace=web-app&path=_&service=web-app&status=500",
					name: "service=web-app",
				},
				{
					path: "k8s.production-cl1.nginx_ingress_controller_response_size_bucket?app_kubernetes_io_component=controller&app_kubernetes_io_instance=ingress-nginx&app_kubernetes_io_managed_by=Helm&app_kubernetes_io_name=ingress-nginx&app_kubernetes_io_version=0_32_0&controller_class=nginx&controller_namespace=ingress-nginx&controller_pod=ingress-nginx-controller-d2ppr&helm_sh_chart=ingress-nginx-2_3_0&host=vm1_test_int&ingress=web-ingress&instance=192_168_0.10&job=kubernetes-service-endpoints&kubernetes_name=ingress-nginx-controller-metrics&kubernetes_namespace=ingress-nginx&kubernetes_node=k8s-n03&le=10&method=GET&namespace=web-app&path=_&service=web-app&status=500",
					name: "status=500",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err error
			base := &Base{
				queue:   make(chan string, 1024),
				inQueue: make(map[string]bool),
				logger:  logger,
				config: &Config{
					TableName:            "test",
					IgnoredTaggedMetrics: tt.ignoredMetrics,
				},
			}
			u := NewTagged(base)
			sort.Strings(tt.wantTags)
			for i := 0; i < len(tt.wantResult); i++ {
				tt.wantResult[i].tags = tt.wantTags
				tt.wantResult[i].days = tt.days
				tt.wantResult[i].version = tt.version
			}
			if err = u.parseName(tt.name, tt.days, tt.version, tag1, wb, tagsBuf); (err != nil) != tt.wantErr {
				t.Errorf("Tagged.parseName() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				r, err := parseTags(wb.Bytes())
				assert.NoError(t, err)
				max := len(tt.wantResult)
				if max < len(r) {
					max = len(r)
				}

				// workaround: tags slice are unsorted
				for i := 0; i < len(r); i++ {
					sort.Strings(r[i].tags)
				}
				sort.Slice(r, func(i, j int) bool { return r[i].name < r[j].name })

				for i := 0; i < max; i++ {
					if i >= len(r) {
						t.Errorf("- result[%d] =\n%s", i, tt.wantResult[i].Dump())
					} else if i >= len(tt.wantResult) {
						t.Errorf("+ result[%d] =\n%s", i, r[i].Dump())
					} else {
						assert.Equalf(t, tt.wantResult[i], r[i], "result[%d]", i)
					}
				}
			}
		})
	}
}

func BenchmarkParseNameShort(b *testing.B) {
	// locate reusable buffers
	tag1 := make([]string, 0)
	wb := RowBinary.GetWriteBuffer()
	tagsBuf := RowBinary.GetWriteBuffer()
	defer wb.Release()
	defer tagsBuf.Release()

	logger := zapwriter.Logger("upload")

	base := &Base{
		queue:   make(chan string, 1024),
		inQueue: make(map[string]bool),
		logger:  logger,
		config:  &Config{TableName: "test"},
	}
	u := NewTagged(base)

	name := "instance:cpu_utilization:ratio_avg?dc=qwe&fqdn=asd&instance=10.33.10.10_9100&job=node"
	for i := 0; i < b.N; i++ {
		version := uint32(time.Now().Unix())
		if err := u.parseName(name, uint16(i), version, tag1, wb, tagsBuf); err != nil {
			b.Fatalf("Tagged.parseName() error = %v", err)
		}
	}
}

func BenchmarkParseNameLong(b *testing.B) {
	// locate reusable buffers
	tag1 := make([]string, 0)
	wb := RowBinary.GetWriteBuffer()
	tagsBuf := RowBinary.GetWriteBuffer()
	defer wb.Release()
	defer tagsBuf.Release()

	logger := zapwriter.Logger("upload")
	base := &Base{
		queue:   make(chan string, 1024),
		inQueue: make(map[string]bool),
		logger:  logger,
		config:  &Config{TableName: "test"},
	}
	u := NewTagged(base)

	name := "k8s.production-cl1.nginx_ingress_controller_response_size_bucket?app_kubernetes_io_component=controller&app_kubernetes_io_instance=ingress-nginx&app_kubernetes_io_managed_by=Helm&app_kubernetes_io_name=ingress-nginx&app_kubernetes_io_version=0_32_0&controller_class=nginx&controller_namespace=ingress-nginx&controller_pod=ingress-nginx-controller-d2ppr&helm_sh_chart=ingress-nginx-2_3_0&host=vm1_test_int&ingress=web-ingress&instance=192_168_0.10&job=kubernetes-service-endpoints&kubernetes_name=ingress-nginx-controller-metrics&kubernetes_namespace=ingress-nginx&kubernetes_node=k8s-n03&le=10&method=GET&namespace=web-app&path=_&service=web-app&status=500"
	for i := 0; i < b.N; i++ {
		version := uint32(time.Now().Unix())
		if err := u.parseName(name, uint16(i), version, tag1, wb, tagsBuf); err != nil {
			b.Fatalf("Tagged.parseName() error = %v", err)
		}
	}
}
