package homekit

import (
	"errors"
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
}

const snapshotTimeout = 5 * time.Second

// getSnapshot asks the HomeKit camera for a JPEG snapshot. The camera
// encodes it in hardware, so there is no decoding here - unlike the
// keyframe + ffmpeg path, which needs a software H264 decoder.
func getSnapshot(rawURL string, width, height int) ([]byte, error) {
	rawURL, _, _ = strings.Cut(rawURL, "#")

	snapshotsMu.Lock()
	sc := snapshots[rawURL]
	if sc == nil {
		sc = &snapshotConn{}
		snapshots[rawURL] = sc
	}
	snapshotsMu.Unlock()

	sc.mu.Lock()
	defer sc.mu.Unlock()

	// retry once - an idle connection may have been dropped by the camera
	for i := 0; ; i++ {
		b, err := sc.getImage(rawURL, width, height)
		if err == nil {
			return checkJPEG(b)
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

func (sc *snapshotConn) getImage(rawURL string, width, height int) ([]byte, error) {
	if sc.client == nil {
		client, err := hap.Dial(rawURL)
		if err != nil {
			return nil, err
		}
		sc.client = client
	}

	_ = sc.client.Conn.SetDeadline(time.Now().Add(snapshotTimeout))
	defer sc.client.Conn.SetDeadline(time.Time{})

	return sc.client.GetImage(width, height)
}

// checkJPEG rejects a camera refusal, which comes as a JSON body
// (ex. {"status":-70412} - not allowed in current state, privacy mode)
func checkJPEG(b []byte) ([]byte, error) {
	if len(b) < 2 || b[0] != 0xFF || b[1] != 0xD8 {
		return nil, errors.New("homekit: snapshot refused: " + string(b))
	}
	return b, nil
}
