// Package certs manages the per-device TLS identity: a self-signed
// certificate whose fingerprint IS the device identity (the Syncthing
// model). Peers are pinned by fingerprint, so there is no CA and no
// DNS/name verification — only the certificate hash matters.
package certs

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"crosssync/internal/version"
)

// Manager holds a device's loaded certificate and key, plus the paths
// they were loaded from (so they can be persisted at first run).
type Manager struct {
	KeyPath  string
	CertPath string
	Cert     *x509.Certificate
	TLSCert  tls.Certificate
}

// LoadOrCreate loads the device certificate from disk, or generates and
// persists a fresh self-signed ed25519 certificate if absent. cn is used
// as the certificate CommonName (typically the device name).
func LoadOrCreate(keyPath, certPath, cn string) (*Manager, error) {
	if m, err := load(keyPath, certPath); err == nil {
		return m, nil
	}
	return create(keyPath, certPath, cn)
}

func load(keyPath, certPath string) (*Manager, error) {
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		return nil, err
	}
	keyPEM, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, err
	}
	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(tlsCert.Certificate[0])
	if err != nil {
		return nil, err
	}
	return &Manager{KeyPath: keyPath, CertPath: certPath, Cert: cert, TLSCert: tlsCert}, nil
}

func create(keyPath, certPath, cn string) (*Manager, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: cn, Organization: []string{"CrossSync"}},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.AddDate(20, 0, 0), // long-lived: identity, not a session credential
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		return nil, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(keyPath), 0o755); err != nil {
		return nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	if err := writeFile(certPath, certPEM, 0o644); err != nil {
		return nil, err
	}
	if err := writeFile(keyPath, keyPEM, 0o600); err != nil {
		return nil, err
	}
	return load(keyPath, certPath)
}

// writeFile writes atomically (temp + rename) so a crash mid-write never
// leaves a truncated cert or key behind.
func writeFile(path string, data []byte, mode os.FileMode) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, mode); err != nil {
		return err
	}
	if err := os.Chmod(tmp, mode); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Fingerprint returns the device fingerprint: lowercase hex SHA-256 of the
// DER-encoded certificate. This is the value configured on peers to pin us.
func (m *Manager) Fingerprint() string {
	sum := sha256.Sum256(m.Cert.Raw)
	return hex.EncodeToString(sum[:])
}

// DeviceID derives the short device ID (first 64 bits of the fingerprint
// hash), the same ID used as the per-device key in version vectors.
func (m *Manager) DeviceID() uint64 {
	sum := sha256.Sum256(m.Cert.Raw)
	return version.DeviceID(sum[:])
}

// ClientConfig builds a TLS 1.3 client config that presents m's certificate
// and accepts only servers whose certificate fingerprint is in allowed.
func ClientConfig(m *Manager, allowed map[string]bool) *tls.Config {
	return &tls.Config{
		Certificates:       []tls.Certificate{m.TLSCert},
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: true, // we pin by fingerprint; no CA chain exists
		VerifyPeerCertificate: verifyFunc(allowed),
	}
}

// ServerConfig builds a TLS 1.3 server config that presents m's certificate,
// requires a client certificate, and accepts only clients whose certificate
// fingerprint is in allowed.
func ServerConfig(m *Manager, allowed map[string]bool) *tls.Config {
	cfg := ClientConfig(m, allowed)
	cfg.ClientAuth = tls.RequireAnyClientCert
	return cfg
}

// verifyFunc returns the certificate-pinning callback: every presented
// certificate is hashed and checked against the allowlist of fingerprints.
func verifyFunc(allowed map[string]bool) func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		for _, raw := range rawCerts {
			c, err := x509.ParseCertificate(raw)
			if err != nil {
				continue
			}
			sum := sha256.Sum256(c.Raw)
			if allowed[hex.EncodeToString(sum[:])] {
				return nil
			}
		}
		return fmt.Errorf("peer certificate fingerprint is not in the allowlist")
	}
}
