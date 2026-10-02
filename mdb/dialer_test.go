package mdb //nolint:testpackage

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAbortOnCloseDialer(t *testing.T) {
	t.Parallel()

	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()

	accepted := make(chan struct{})
	go func() {
		defer close(accepted)

		conn, aerr := ln.Accept()
		if aerr == nil {
			_ = conn.Close()
		}
	}()

	d := &abortOnCloseDialer{}

	conn, err := d.DialContext(t.Context(), "tcp", ln.Addr().String())
	require.NoError(t, err)
	require.IsType(t, &net.TCPConn{}, conn)
	require.NoError(t, conn.Close())
	<-accepted

	_, err = d.DialContext(t.Context(), "tcp", "127.0.0.1:1")
	require.Error(t, err)
}
