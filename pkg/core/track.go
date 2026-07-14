package core

import (
	"encoding/json"
	"errors"

	"github.com/pion/rtp"
)

var ErrCantGetTrack = errors.New("can't get track")

// KeyframeRTP - RTP payload keyframe detectors, registered by codec packages.
// Receiver with a detector keeps a GOP cache - all packets since the last
// keyframe - and replays it to every new sender, so consumers don't have to
// wait for the next keyframe from sources that never send them on demand.
var KeyframeRTP = map[string]func(payload []byte) bool{}

// gopMaxPackets protects from unbound cache growth when the stream
// has huge or missing keyframe intervals
const gopMaxPackets = 4096

type Receiver struct {
	Node

	// Deprecated: should be removed
	Media *Media `json:"-"`
	// Deprecated: should be removed
	ID byte `json:"-"` // Channel for RTSP, PayloadType for MPEG-TS

	Bytes   int `json:"bytes,omitempty"`
	Packets int `json:"packets,omitempty"`

	gop     []*Packet
	gopSkip bool // wait for next keyframe (start or after overflow)
	gopKey  bool // currently inside a key unit (SPS/PPS/IDR sequence)
	keyFunc func(payload []byte) bool
}

func NewReceiver(media *Media, codec *Codec) *Receiver {
	r := &Receiver{
		Node:  Node{id: NewID(), Codec: codec},
		Media: media,
	}

	if codec != nil && codec.IsRTP() {
		r.keyFunc = KeyframeRTP[codec.Name]
	}

	if r.keyFunc == nil {
		r.Input = func(packet *Packet) {
			r.Bytes += len(packet.Payload)
			r.Packets++
			for _, child := range r.childs {
				child.Input(packet)
			}
		}
		return r
	}

	r.gopSkip = true // cache starts on first keyframe

	r.Input = func(packet *Packet) {
		r.Bytes += len(packet.Payload)
		r.Packets++

		// GOP cache and childs update under same mutex as bindSender,
		// so new senders never miss or duplicate packets
		r.mu.Lock()
		if r.keyFunc(packet.Payload) {
			if !r.gopKey {
				// transition into a new key unit starts a new cache
				// (consecutive SPS/PPS/IDR packets stay together)
				r.gop = r.gop[:0]
				r.gopKey = true
				r.gopSkip = false
			}
		} else {
			r.gopKey = false
		}
		if !r.gopSkip {
			if len(r.gop) < gopMaxPackets {
				r.gop = append(r.gop, packet)
			} else {
				r.gop = r.gop[:0]
				r.gopSkip = true
				r.gopKey = false
			}
		}
		for _, child := range r.childs {
			child.Input(packet)
		}
		r.mu.Unlock()
	}
	return r
}

// bindSender attaches sender and replays the GOP cache into it,
// so the new consumer starts from a keyframe without waiting
func (r *Receiver) bindSender(s *Sender) {
	r.mu.Lock()
	for _, packet := range r.gop {
		s.Input(packet)
	}
	r.childs = append(r.childs, &s.Node)
	r.mu.Unlock()

	s.parent = &r.Node
}

// Deprecated: should be removed
func (r *Receiver) WriteRTP(packet *rtp.Packet) {
	r.Input(packet)
}

// Deprecated: should be removed
func (r *Receiver) Senders() []*Sender {
	if len(r.childs) > 0 {
		return []*Sender{{}}
	} else {
		return nil
	}
}

// Deprecated: should be removed
func (r *Receiver) Replace(target *Receiver) {
	MoveNode(&target.Node, &r.Node)
}

func (r *Receiver) Close() {
	r.Node.Close()
}

type Sender struct {
	Node

	// Deprecated:
	Media *Media `json:"-"`
	// Deprecated:
	Handler HandlerFunc `json:"-"`

	Bytes   int `json:"bytes,omitempty"`
	Packets int `json:"packets,omitempty"`
	Drops   int `json:"drops,omitempty"`

	buf  chan *Packet
	done chan struct{}
}

func NewSender(media *Media, codec *Codec) *Sender {
	var bufSize uint16

	if GetKind(codec.Name) == KindVideo {
		if codec.IsRTP() {
			// in my tests 40Mbit/s 4K-video can generate up to 1500 items
			// for the h264.RTPDepay => RTPPay queue
			bufSize = 4096
		} else {
			bufSize = 64
		}
	} else {
		bufSize = 128
	}

	buf := make(chan *Packet, bufSize)
	s := &Sender{
		Node:  Node{id: NewID(), Codec: codec},
		Media: media,
		buf:   buf,
	}
	s.Input = func(packet *Packet) {
		s.mu.Lock()
		// unblock write to nil chan - OK, write to closed chan - panic
		select {
		case s.buf <- packet:
			s.Bytes += len(packet.Payload)
			s.Packets++
		default:
			s.Drops++
		}
		s.mu.Unlock()
	}
	s.Output = func(packet *Packet) {
		s.Handler(packet)
	}
	return s
}

// Deprecated: should be removed
func (s *Sender) HandleRTP(parent *Receiver) {
	parent.bindSender(s)
	s.Start()
}

// Deprecated: should be removed
func (s *Sender) Bind(parent *Receiver) {
	s.WithParent(parent)
}

func (s *Sender) WithParent(parent *Receiver) *Sender {
	s.Node.WithParent(&parent.Node)
	return s
}

func (s *Sender) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.buf == nil || s.done != nil {
		return
	}
	s.done = make(chan struct{})

	// pass buf directly so that it's impossible for buf to be nil
	go func(buf chan *Packet) {
		for packet := range buf {
			s.Output(packet)
		}
		close(s.done)
	}(s.buf)
}

func (s *Sender) Wait() {
	if done := s.done; done != nil {
		<-done
	}
}

func (s *Sender) State() string {
	if s.buf == nil {
		return "closed"
	}
	if s.done == nil {
		return "new"
	}
	return "connected"
}

func (s *Sender) Close() {
	// close buffer if exists
	s.mu.Lock()
	if s.buf != nil {
		close(s.buf) // exit from for range loop
		s.buf = nil  // prevent writing to closed chan
	}
	s.mu.Unlock()

	s.Node.Close()
}

func (r *Receiver) MarshalJSON() ([]byte, error) {
	v := struct {
		ID      uint32   `json:"id"`
		Codec   *Codec   `json:"codec"`
		Childs  []uint32 `json:"childs,omitempty"`
		Bytes   int      `json:"bytes,omitempty"`
		Packets int      `json:"packets,omitempty"`
	}{
		ID:      r.Node.id,
		Codec:   r.Node.Codec,
		Bytes:   r.Bytes,
		Packets: r.Packets,
	}
	for _, child := range r.childs {
		v.Childs = append(v.Childs, child.id)
	}
	return json.Marshal(v)
}

func (s *Sender) MarshalJSON() ([]byte, error) {
	v := struct {
		ID      uint32 `json:"id"`
		Codec   *Codec `json:"codec"`
		Parent  uint32 `json:"parent,omitempty"`
		Bytes   int    `json:"bytes,omitempty"`
		Packets int    `json:"packets,omitempty"`
		Drops   int    `json:"drops,omitempty"`
	}{
		ID:      s.Node.id,
		Codec:   s.Node.Codec,
		Bytes:   s.Bytes,
		Packets: s.Packets,
		Drops:   s.Drops,
	}
	if s.parent != nil {
		v.Parent = s.parent.id
	}
	return json.Marshal(v)
}
