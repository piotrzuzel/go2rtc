package streams

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
)

type state byte

const (
	stateNone state = iota
	stateMedias
	stateTracks
	stateStart
	stateExternal
	stateInternal
)

type Producer struct {
	core.Listener

	url      string
	template string

	conn      core.Producer
	receivers []*core.Receiver
	senders   []*core.Receiver

	state    state
	mu       sync.Mutex
	workerID int
}

const SourceTemplate = "{input}"

func NewProducer(source string) *Producer {
	if strings.Contains(source, SourceTemplate) {
		return &Producer{template: source}
	}

	return &Producer{url: source}
}

func (p *Producer) SetSource(s string) {
	if p.template == "" {
		p.url = s
	} else {
		p.url = strings.Replace(p.template, SourceTemplate, s, 1)
	}
}

func (p *Producer) Dial() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state == stateNone {
		conn, err := GetProducer(p.url)
		if err != nil {
			return err
		}

		p.conn = conn
		p.state = stateMedias
	}

	return nil
}

func (p *Producer) GetMedias() []*core.Media {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conn == nil {
		return nil
	}

	return p.conn.GetMedias()
}

func (p *Producer) GetTrack(media *core.Media, codec *core.Codec) (*core.Receiver, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state == stateNone {
		return nil, errors.New("get track from none state")
	}

	for _, track := range p.receivers {
		if track.Codec == codec {
			// new consumer for already running track shouldn't wait for keyframe
			if req, ok := p.conn.(interface{ RequestKeyframe() }); ok && codec.IsVideo() {
				req.RequestKeyframe()
			}
			return track, nil
		}
	}

	track, err := p.conn.GetTrack(media, codec)
	if err != nil {
		return nil, err
	}

	p.receivers = append(p.receivers, track)

	if p.state == stateMedias {
		p.state = stateTracks
	}

	return track, nil
}

func (p *Producer) AddTrack(media *core.Media, codec *core.Codec, track *core.Receiver) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state == stateNone {
		return errors.New("add track from none state")
	}

	if err := p.conn.(core.Consumer).AddTrack(media, codec, track); err != nil {
		return err
	}

	p.senders = append(p.senders, track)

	if p.state == stateMedias {
		p.state = stateTracks
	}

	return nil
}

func (p *Producer) MarshalJSON() ([]byte, error) {
	if conn := p.conn; conn != nil {
		return json.Marshal(conn)
	}
	info := map[string]string{"url": p.url}
	return json.Marshal(info)
}

// internals

func (p *Producer) start() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state != stateTracks {
		return
	}

	log.Debug().Msgf("[streams] start producer url=%s", p.url)

	p.state = stateStart
	p.workerID++

	go p.worker(p.conn, p.workerID, 0)
}

func retryTimeout(retry int) time.Duration {
	switch {
	case retry < 5:
		return time.Second
	case retry < 10:
		return time.Second * 5
	case retry < 20:
		return time.Second * 10
	}
	return time.Minute
}

func (p *Producer) worker(conn core.Producer, workerID, retry int) {
	startTime := time.Now()

	if err := conn.Start(); err != nil {
		p.mu.Lock()
		closed := p.workerID != workerID
		p.mu.Unlock()

		if closed {
			return
		}

		log.Warn().Err(err).Str("url", p.url).Caller().Send()
	}

	// producer that fails quickly should backoff before reconnect,
	// so it doesn't flood the source device (some cameras can wedge)
	if time.Since(startTime) > time.Minute {
		retry = 0
	} else {
		retry++
	}

	if retry > 0 {
		time.AfterFunc(retryTimeout(retry), func() {
			p.reconnect(workerID, retry)
		})
	} else {
		p.reconnect(workerID, retry)
	}
}

func (p *Producer) reconnect(workerID, retry int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.workerID != workerID {
		log.Trace().Msgf("[streams] stop reconnect url=%s", p.url)
		return
	}

	log.Debug().Msgf("[streams] retry=%d to url=%s", retry, p.url)

	conn, err := GetProducer(p.url)
	if err != nil {
		log.Debug().Msgf("[streams] producer=%s", err)

		time.AfterFunc(retryTimeout(retry), func() {
			p.reconnect(workerID, retry+1)
		})
		return
	}

	for _, media := range conn.GetMedias() {
		switch media.Direction {
		case core.DirectionRecvonly:
			for i, receiver := range p.receivers {
				codec := media.MatchCodec(receiver.Codec)
				if codec == nil {
					continue
				}

				track, err := conn.GetTrack(media, codec)
				if err != nil {
					continue
				}

				receiver.Replace(track)
				p.receivers[i] = track
				break
			}

		case core.DirectionSendonly:
			for _, sender := range p.senders {
				codec := media.MatchCodec(sender.Codec)
				if codec == nil {
					continue
				}

				_ = conn.(core.Consumer).AddTrack(media, codec, sender)
			}
		}
	}

	// stop previous connection after moving tracks (fix ghost exec/ffmpeg)
	_ = p.conn.Stop()
	// swap connections
	p.conn = conn

	go p.worker(conn, workerID, retry)
}

func (p *Producer) stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	switch p.state {
	case stateExternal:
		log.Trace().Msgf("[streams] skip stop external producer")
		return
	case stateNone:
		log.Trace().Msgf("[streams] skip stop none producer")
		return
	case stateStart:
		p.workerID++
	}

	log.Debug().Msgf("[streams] stop producer url=%s", p.url)

	if p.conn != nil {
		_ = p.conn.Stop()
		p.conn = nil
	}

	p.state = stateNone
	p.receivers = nil
	p.senders = nil
}
