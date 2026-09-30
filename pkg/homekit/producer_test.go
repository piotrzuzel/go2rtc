package homekit

import (
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/hap"
	"github.com/stretchr/testify/require"
)

// a camera that never answers must not block the producer forever
func TestWithDeadlineUnblocksSilentCamera(t *testing.T) {
	requestTimeout = 100 * time.Millisecond
	defer func() { requestTimeout = 5 * time.Second }()

	conn, camera := net.Pipe() // camera side never writes
	defer camera.Close()
	defer conn.Close()

	c := &Client{hap: &hap.Client{Conn: conn}}

	start := time.Now()
	err := c.withDeadline(func() error {
		_, err := conn.Read(make([]byte, 1))
		return err
	})
	require.True(t, errors.Is(err, os.ErrDeadlineExceeded), "got %v", err)
	require.Less(t, time.Since(start), time.Second)

	// the deadline is cleared, so a lingering connection stays usable
	go func() { _, _ = camera.Write([]byte{1}) }()
	time.Sleep(2 * requestTimeout)
	_, err = conn.Read(make([]byte, 1))
	require.NoError(t, err)
}
