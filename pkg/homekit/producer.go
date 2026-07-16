package homekit

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/hap"
	"github.com/AlexxIT/go2rtc/pkg/hap/camera"
	"github.com/AlexxIT/go2rtc/pkg/srtp"
	"github.com/pion/rtp"
)

// Deprecated: rename to Producer
type Client struct {
	core.Connection

	hap  *hap.Client
	srtp *srtp.Server

	videoConfig camera.SupportedVideoStreamConfiguration
	audioConfig camera.SupportedAudioStreamConfiguration

	videoSession *srtp.Session
	audioSession *srtp.Session

	stream *camera.Stream

	MaxWidth  int `json:"-"`
	MaxHeight int `json:"-"`
	Bitrate   int `json:"-"` // in bits/s
}

func Dial(rawURL string, server *srtp.Server) (*Client, error) {
	// reuse lingering connection from previous session if alive
	item := poolGet(rawURL)
	if item == nil {
		conn, err := hap.Dial(rawURL)
		if err != nil {
			return nil, err
		}
		item = &poolItem{client: conn}
	}

	client := &Client{
		Connection: core.Connection{
			ID:         core.NewID(),
			FormatName: "homekit",
			Protocol:   "udp",
			RemoteAddr: item.client.Conn.RemoteAddr().String(),
			Source:     rawURL,
			Transport:  item.client,
			Medias:     item.medias,
		},
		hap:         item.client,
		srtp:        server,
		videoConfig: item.videoConfig,
		audioConfig: item.audioConfig,
	}

	return client, nil
}

func (c *Client) Conn() net.Conn {
	return c.hap.Conn
}

func (c *Client) GetMedias() []*core.Media {
	if c.Medias != nil {
		return c.Medias
	}

	acc, err := c.hap.GetFirstAccessory()
	if err != nil {
		return nil
	}

	char := acc.GetCharacter(camera.TypeSupportedVideoStreamConfiguration)
	if char == nil {
		return nil
	}
	if err = char.ReadTLV8(&c.videoConfig); err != nil {
		return nil
	}

	char = acc.GetCharacter(camera.TypeSupportedAudioStreamConfiguration)
	if char == nil {
		return nil
	}
	if err = char.ReadTLV8(&c.audioConfig); err != nil {
		return nil
	}

	c.SDP = fmt.Sprintf("%+v\n%+v", c.videoConfig, c.audioConfig)

	c.Medias = []*core.Media{
		videoToMedia(c.videoConfig.Codecs),
		audioToMedia(c.audioConfig.Codecs),
		{
			Kind:      core.KindVideo,
			Direction: core.DirectionRecvonly,
			Codecs: []*core.Codec{
				{
					Name:        core.CodecJPEG,
					ClockRate:   90000,
					PayloadType: core.PayloadTypeRAW,
				},
			},
		},
	}

	return c.Medias
}

// Hooks for mirroring camera state into accessories served by go2rtc.
// Wired by internal/homekit; source is the producer source URL.
var (
	OnSourceState  func(source string, active bool)
	OnSourceMotion func(source string, motion bool)
)

func (c *Client) Start() error {
	if c.Receivers == nil {
		return errors.New("producer without tracks")
	}

	if c.Receivers[0].Codec.Name == core.CodecJPEG {
		return c.startMJPEG()
	}

	if len(c.videoConfig.Codecs) == 0 || len(c.audioConfig.Codecs) == 0 {
		return errors.New("homekit: missing stream configuration")
	}

	videoTrack := c.trackByKind(core.KindVideo)
	videoCodec := trackToVideo(videoTrack, &c.videoConfig.Codecs[0], c.MaxWidth, c.MaxHeight)

	audioTrack := c.trackByKind(core.KindAudio)
	audioCodec := trackToAudio(audioTrack, &c.audioConfig.Codecs[0])

	c.videoSession = &srtp.Session{Local: c.srtpEndpoint()}
	c.audioSession = &srtp.Session{Local: c.srtpEndpoint()}

	var err error
	c.stream, err = camera.NewStream(c.hap, videoCodec, audioCodec, c.videoSession, c.audioSession, c.Bitrate)
	if err != nil {
		return err
	}

	c.srtp.AddSession(c.videoSession)
	c.srtp.AddSession(c.audioSession)

	// mirror camera state and motion events into served accessories
	go c.watchEvents()
	defer func() {
		if OnSourceState != nil {
			OnSourceState(c.Source, false)
		}
	}()

	var activeOnce sync.Once
	notifyActive := func() {
		activeOnce.Do(func() {
			if OnSourceState != nil {
				OnSourceState(c.Source, true)
			}
		})
	}

	deadline := time.NewTimer(core.ConnDeadline)

	if videoTrack != nil {
		c.videoSession.OnReadRTP = func(packet *rtp.Packet) {
			deadline.Reset(core.ConnDeadline)
			notifyActive()
			videoTrack.WriteRTP(packet)
			c.Recv += len(packet.Payload)
		}

		if audioTrack != nil {
			c.audioSession.OnReadRTP = func(packet *rtp.Packet) {
				audioTrack.WriteRTP(packet)
				c.Recv += len(packet.Payload)
			}
		}
	} else {
		c.audioSession.OnReadRTP = func(packet *rtp.Packet) {
			deadline.Reset(core.ConnDeadline)
			notifyActive()
			audioTrack.WriteRTP(packet)
			c.Recv += len(packet.Payload)
		}
	}

	if c.audioSession.OnReadRTP != nil {
		c.audioSession.OnReadRTP = timekeeper(c.audioSession.OnReadRTP)
	}

	<-deadline.C

	return nil
}

func (c *Client) Stop() error {
	if c.videoSession != nil && c.videoSession.Remote != nil {
		c.srtp.DelSession(c.videoSession)
	}
	if c.audioSession != nil && c.audioSession.Remote != nil {
		c.srtp.DelSession(c.audioSession)
	}

	// end camera RTP session but keep the pair-verified connection
	// for near instant next stream start (camera sends keyframe on start)
	if c.stream != nil && c.stream.Close() == nil {
		poolPut(c.Source, &poolItem{
			client:      c.hap,
			medias:      c.Medias,
			videoConfig: c.videoConfig,
			audioConfig: c.audioConfig,
		})

		for _, receiver := range c.Receivers {
			receiver.Close()
		}
		for _, sender := range c.Senders {
			sender.Close()
		}
		return nil
	}

	return c.Connection.Stop()
}

// watchEvents subscribes to the camera motion sensor and pumps
// unsolicited EVENT frames from the camera to the mirror hooks
func (c *Client) watchEvents() {
	acc, err := c.hap.GetFirstAccessory()
	if err != nil {
		return
	}

	var motionIID uint64
	for _, srv := range acc.Services {
		if srv.Type == "85" { // MotionSensor
			if char := srv.GetCharacter("22"); char != nil { // MotionDetected
				motionIID = char.IID
			}
		}
	}
	if motionIID == 0 {
		return // camera without motion sensor
	}

	c.hap.OnEvent = func(res *http.Response) {
		var v hap.JSONCharacters
		if err := json.NewDecoder(res.Body).Decode(&v); err != nil {
			return
		}
		for _, char := range v.Value {
			if char.IID != motionIID {
				continue
			}
			var motion bool
			switch val := char.Value.(type) {
			case bool:
				motion = val
			case float64:
				motion = val != 0
			}
			if OnSourceMotion != nil {
				OnSourceMotion(c.Source, motion)
			}
		}
	}

	if err = c.hap.SubscribeEvents(motionIID); err != nil {
		return
	}

	// background reader delivers camera events even between sessions
	// (lingering connection)
	c.hap.StartEvents()
}

// RequestKeyframe - new consumers should wait less for the first frame,
// some cameras may ignore this request (ex. Tapo C225)
func (c *Client) RequestKeyframe() {
	if c.videoSession != nil && c.videoSession.Remote != nil {
		c.videoSession.RequestKeyframe()
	}
}

func (c *Client) trackByKind(kind string) *core.Receiver {
	for _, receiver := range c.Receivers {
		if receiver.Codec.Kind() == kind {
			return receiver
		}
	}
	return nil
}

func (c *Client) startMJPEG() error {
	receiver := c.Receivers[0]

	for {
		b, err := c.hap.GetImage(1920, 1080)
		if err != nil {
			return err
		}

		c.Recv += len(b)

		packet := &rtp.Packet{
			Header:  rtp.Header{Timestamp: core.Now90000()},
			Payload: b,
		}
		receiver.WriteRTP(packet)
	}
}

func (c *Client) srtpEndpoint() *srtp.Endpoint {
	return &srtp.Endpoint{
		Addr:       c.hap.LocalIP(),
		Port:       uint16(c.srtp.Port()),
		MasterKey:  []byte(core.RandString(16, 0)),
		MasterSalt: []byte(core.RandString(14, 0)),
		SSRC:       rand.Uint32(),
	}
}

func timekeeper(handler core.HandlerFunc) core.HandlerFunc {
	const sampleRate = 16000
	const sampleSize = 480

	var send time.Duration
	var firstTime time.Time

	return func(packet *rtp.Packet) {
		now := time.Now()

		if send != 0 {
			elapsed := now.Sub(firstTime) * sampleRate / time.Second
			if send+sampleSize > elapsed {
				return // drop overflow frame
			}
		} else {
			firstTime = now
		}

		send += sampleSize

		packet.Timestamp = uint32(send)

		handler(packet)
	}
}
