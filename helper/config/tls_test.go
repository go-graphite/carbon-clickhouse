package config

import (
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseTLSVersion(t *testing.T) {
	tests := []struct {
		name    string
		version string
		want    uint16
		wantErr bool
	}{
		{name: "TLS1.0", version: "TLS1.0", want: tls.VersionTLS10, wantErr: false},
		{name: "TLS1.0 with spaces", version: "TLS 1.0", want: tls.VersionTLS10, wantErr: false},
		{name: "TLS10", version: "TLS10", want: tls.VersionTLS10, wantErr: false},
		{name: "TLS10 with spaces", version: "TLS 10", want: tls.VersionTLS10, wantErr: false},
		{name: "VersionTLS10", version: "VersionTLS10", want: tls.VersionTLS10, wantErr: false},
		{name: "Version TLS1.0", version: "Version TLS1.0", want: tls.VersionTLS10, wantErr: false},
		{name: "VersionTLS11", version: "VersionTLS11", want: tls.VersionTLS11, wantErr: false},
		{name: "VersionTLS12", version: "VersionTLS12", want: tls.VersionTLS12, wantErr: false},
		{name: "TLS13", version: "TLS13", want: tls.VersionTLS13, wantErr: false},
		{name: "Invalid version should err", version: "something", want: 0, wantErr: true},
		{name: "unsupported version should err", version: "VersionSSL30", want: 0, wantErr: true},
		{name: "empty version returns latest", version: "", want: tls.VersionTLS13, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTLSVersion(tt.version)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
