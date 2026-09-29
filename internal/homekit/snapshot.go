package homekit

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/hap"
)

// snapshot connections to HomeKit cameras, one per camera URL. Kept apart
// from the stream producer, so snapshots don't depend on the stream state
// and don't interleave with its session setup requests.
var snapshots = map[string]*snapshotConn{}
var snapshotsMu sync.Mutex

type snapshotConn struct {
	mu     sync.Mutex
	client *hap.Client

	// last camera answer - JPEG or refusal
	answered time.Time
	refused  bool
}

const (
	snapshotTimeout = 5 * time.Second
	probeTimeout    = time.Second
	probeMaxAge     = 10 * time.Second
)

var errSnapshotRefused = errors.New("homekit: snapshot refused")

func getSnapshotConn(rawURL string) *snapshotConn {
	rawURL, _, _ = strings.Cut(rawURL, "#")

	snapshotsMu.Lock()
	defer snapshotsMu.Unlock()

	sc := snapshots[rawURL]
	if sc == nil {
		sc = &snapshotConn{}
		snapshots[rawURL] = sc
	}
	return sc
}

// getSnapshot asks the HomeKit camera for a JPEG snapshot. The camera
// encodes it in hardware, so there is no decoding here - unlike the
// keyframe + ffmpeg path, which needs a software H264 decoder.
func getSnapshot(rawURL string, width, height int) ([]byte, error) {
	return getSnapshotConn(rawURL).get(rawURL, width, height, snapshotTimeout)
}

// cameraRefuses reports whether the HomeKit camera refuses to serve
// (privacy mode). The camera keeps advertising itself as available and
// only refuses requests. Apple Home asks for a snapshot right before
// opening the stream, so the last answer is usually fresh; otherwise ask
// for a small one. Connection problems don't count - the stream is then
// tried as usual.
func cameraRefuses(rawURL string) bool {
	sc := getSnapshotConn(rawURL)

	sc.mu.Lock()
	if time.Since(sc.answered) < probeMaxAge {
		defer sc.mu.Unlock()
		return sc.refused
	}
	sc.mu.Unlock()

	_, err := sc.get(rawURL, 320, 180, probeTimeout)
	return errors.Is(err, errSnapshotRefused)
}

func (sc *snapshotConn) get(rawURL string, width, height int, timeout time.Duration) ([]byte, error) {
	rawURL, _, _ = strings.Cut(rawURL, "#")

	sc.mu.Lock()
	defer sc.mu.Unlock()

	// retry once - an idle connection may have been dropped by the camera
	for i := 0; ; i++ {
		b, err := sc.getImage(rawURL, width, height, timeout)
		if err == nil {
			b, err = checkJPEG(b)
			sc.answered = time.Now()
			sc.refused = err != nil
			return b, err
		}
		if sc.client != nil {
			_ = sc.client.Close()
			sc.client = nil
		}
		if i > 0 {
			return nil, err
		}
	}
}

func (sc *snapshotConn) getImage(rawURL string, width, height int, timeout time.Duration) ([]byte, error) {
	if sc.client == nil {
		client, err := hap.Dial(rawURL)
		if err != nil {
			return nil, err
		}
		sc.client = client
	}

	_ = sc.client.Conn.SetDeadline(time.Now().Add(timeout))
	defer sc.client.Conn.SetDeadline(time.Time{})

	return sc.client.GetImage(width, height)
}

// checkJPEG rejects a camera refusal, which comes as a JSON body
// (ex. {"status":-70412} - not allowed in current state, privacy mode)
func checkJPEG(b []byte) ([]byte, error) {
	if len(b) < 2 || b[0] != 0xFF || b[1] != 0xD8 {
		return nil, fmt.Errorf("%w: %s", errSnapshotRefused, b)
	}
	return b, nil
}
