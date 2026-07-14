package homekit

import (
	"io"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/hap"
	"github.com/AlexxIT/go2rtc/pkg/hap/camera"
)

// pool keeps pair-verified connections to cameras after the last consumer
// leaves, so the next stream start skips discovery, dial and pair-verify.
// Cameras send a keyframe on every stream start, so a lingering connection
// gives near instant first frame without any standby video traffic.
type poolItem struct {
	client      *hap.Client
	medias      []*core.Media
	videoConfig camera.SupportedVideoStreamConfiguration
	audioConfig camera.SupportedAudioStreamConfiguration
}

var pool = map[string]*poolItem{}
var poolMu sync.Mutex

// poolGet returns a verified connection for the source or nil.
// Item is removed from the pool - single owner at a time.
func poolGet(rawURL string) *poolItem {
	poolMu.Lock()
	item, ok := pool[rawURL]
	if ok {
		delete(pool, rawURL)
	}
	poolMu.Unlock()

	if !ok {
		return nil
	}

	// camera may silently close idle connection - check with deadline
	_ = item.client.Conn.SetDeadline(time.Now().Add(2 * time.Second))
	res, err := item.client.Get(hap.PathAccessories)
	if err == nil {
		_, err = io.Copy(io.Discard, res.Body)
	}
	if err != nil {
		_ = item.client.Close()
		return nil
	}
	_ = item.client.Conn.SetDeadline(time.Time{})

	return item
}

func poolPut(rawURL string, item *poolItem) {
	poolMu.Lock()
	if old, ok := pool[rawURL]; ok {
		_ = old.client.Close()
	}
	pool[rawURL] = item
	poolMu.Unlock()
}
