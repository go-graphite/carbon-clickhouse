package config

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"strings"
)

type TLS struct {
	Certificates []CertificatePair `toml:"certificates"`
	CACertFiles  []string          `toml:"ca-cert"`

	ServerName         string `toml:"server-name"`
	MinVersion         string `toml:"min-version"` // supported formats: TLS10, TLS11, TLS12, TLS13
	MaxVersion         string `toml:"max-version"` // supported formats: TLS10, TLS11, TLS12, TLS13
	InsecureSkipVerify bool   `toml:"insecure-skip-verify"`
}

type CertificatePair struct {
	KeyFile  string `toml:"key"`
	CertFile string `toml:"cert"`
}

// ParseClientTLSConfig returns TLS config for HTTPS client's mTLS connections.
func ParseClientTLSConfig(config *TLS) (*tls.Config, error) {
	if len(config.Certificates) == 0 {
		return nil, errors.New("no tls certificates provided")
	}

	caCertPool := x509.NewCertPool()
	for _, caCert := range config.CACertFiles {
		cert, err := os.ReadFile(caCert)
		if err != nil {
			return nil, err
		}
		caCertPool.AppendCertsFromPEM(cert)
	}

	certificates := make([]tls.Certificate, 0, len(config.Certificates))
	for _, it := range config.Certificates {
		cert, err := tls.LoadX509KeyPair(it.CertFile, it.KeyFile)
		if err != nil {
			return nil, err
		}
		certificates = append(certificates, cert)
	}

	minVersion, err := ParseTLSVersion(config.MinVersion)
	if err != nil {
		return nil, err
	}
	maxVersion, err := ParseTLSVersion(config.MaxVersion)
	if err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{
		RootCAs:            caCertPool,
		Certificates:       certificates,
		ServerName:         config.ServerName,
		MinVersion:         minVersion,
		MaxVersion:         maxVersion,
		InsecureSkipVerify: config.InsecureSkipVerify,
	}
	return tlsConfig, nil
}

// ParseTLSVersion converts a TLS version string ("TLS10", "TLS11", "TLS12", "TLS13")
// to its respective uint16 constant.
// An empty string defaults to TLS version 1.3.
//
// Returns an error for unknown or invalid versions.
func ParseTLSVersion(version string) (uint16, error) {
	replacer := strings.NewReplacer("Version", "", ".", "", " ", "")
	version = replacer.Replace(version)
	switch version {
	case "TLS10":
		return tls.VersionTLS10, nil
	case "TLS11":
		return tls.VersionTLS11, nil
	case "TLS12":
		return tls.VersionTLS12, nil
	case "TLS13", "":
		return tls.VersionTLS13, nil
	default:
		return 0, errors.New("unknown TLS version")
	}
}
