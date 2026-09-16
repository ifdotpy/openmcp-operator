package disconnectguard

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"time"
)

// Serve reloads the certificate for each handshake so projected certificates can rotate.
func Serve(ctx context.Context, address, certFile, keyFile string, handler http.Handler) error {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		return &cert, err
	}}
	// Fail before accepting traffic when the initial certificate is invalid.
	if _, err := cfg.GetCertificate(nil); err != nil {
		return err
	}
	server := &http.Server{Addr: address, Handler: handler, TLSConfig: cfg, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = server.Shutdown(shutdown)
		case <-done:
		}
	}()
	defer close(done)
	err := server.ListenAndServeTLS("", "")
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
