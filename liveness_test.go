package sock

import (
	"context"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// blackholeProxy forwards TCP to target until blackhole is set, then keeps
// both sockets open and silently discards everything: a half-dead connection,
// as a NAT that forgot the flow or a laptop that slept leaves behind.
func blackholeProxy(t *testing.T, target string) (addr string, blackhole *atomic.Bool) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	blackhole = new(atomic.Bool)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			s, err := net.Dial("tcp", target)
			if err != nil {
				_ = c.Close()
				continue
			}
			t.Cleanup(func() { _ = c.Close(); _ = s.Close() })
			pipe := func(dst, src net.Conn) {
				buf := make([]byte, 32<<10)
				for {
					n, err := src.Read(buf)
					if n > 0 && !blackhole.Load() {
						_, _ = dst.Write(buf[:n])
					}
					if err != nil {
						return
					}
				}
			}
			go pipe(s, c)
			go pipe(c, s)
		}
	}()
	return ln.Addr().String(), blackhole
}

// A stream whose connection dies silently is noticed within seconds by the
// transport's own health pings, not after the 60s silence watchdog.
func TestHalfDeadStreamIsDetectedWithinSeconds(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())

	addr, blackhole := blackholeProxy(t, srv.Listener.Addr().String())
	tr := newTransport(liveness{ping: 200 * time.Millisecond, pingTimeout: 300 * time.Millisecond, tcpIdle: time.Second, tcpInterval: time.Second, tcpCount: 2})
	tr.TLSClientConfig.RootCAs = pool
	tr.TLSClientConfig.ServerName = "example.com"
	t.Cleanup(tr.CloseIdleConnections)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+addr+"/", nil)
	res, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.ProtoMajor != 2 {
		t.Fatalf("proto %s, want HTTP/2", res.Proto)
	}
	blackhole.Store(true)
	start := time.Now()
	_, err = io.Copy(io.Discard, res.Body)
	if err == nil {
		t.Fatal("the dead stream ended without an error")
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("detected after %v, want within seconds", took)
	}
}

// The defaults are what ships: a ping after 5s of silence, closed 3s later.
func TestDefaultLivenessIsAFewSeconds(t *testing.T) {
	d := defaultLiveness
	if d.ping != 5*time.Second || d.pingTimeout != 3*time.Second || d.ping+d.pingTimeout > 10*time.Second {
		t.Fatalf("defaults %+v", d)
	}
	tr := newTransport(d)
	if tr.HTTP2 == nil || tr.HTTP2.SendPingTimeout != d.ping || tr.HTTP2.PingTimeout != d.pingTimeout {
		t.Fatalf("HTTP2 config %+v", tr.HTTP2)
	}
}
