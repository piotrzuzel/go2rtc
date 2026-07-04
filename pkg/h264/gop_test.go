package h264

import (
	"testing"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"
)

func h264Packet(seq uint16, naluType byte) *rtp.Packet {
	return &rtp.Packet{
		Header:  rtp.Header{SequenceNumber: seq, PayloadType: 96},
		Payload: []byte{naluType, 0x00, 0x00},
	}
}

func TestGOPCacheReplay(t *testing.T) {
	codec := &core.Codec{Name: core.CodecH264, ClockRate: 90000, PayloadType: 96}
	media := &core.Media{Kind: core.KindVideo, Direction: core.DirectionRecvonly}

	receiver := core.NewReceiver(media, codec)

	// stream: P (before any keyframe), SPS, PPS, IDR, P, P
	receiver.WriteRTP(h264Packet(1, NALUTypePFrame)) // must not be cached (no keyframe yet)
	receiver.WriteRTP(h264Packet(2, NALUTypeSPS))
	receiver.WriteRTP(h264Packet(3, NALUTypePPS))
	receiver.WriteRTP(h264Packet(4, NALUTypeIFrame))
	receiver.WriteRTP(h264Packet(5, NALUTypePFrame))
	receiver.WriteRTP(h264Packet(6, NALUTypePFrame))

	var got []uint16
	done := make(chan struct{})

	sender := core.NewSender(media, codec)
	sender.Handler = func(packet *rtp.Packet) {
		got = append(got, packet.SequenceNumber)
		if len(got) == 5 {
			close(done)
		}
	}
	sender.HandleRTP(receiver)

	<-done
	// replay must start at SPS and include everything since
	require.Equal(t, []uint16{2, 3, 4, 5, 6}, got)

	// next keyframe resets the cache
	receiver.WriteRTP(h264Packet(7, NALUTypeSPS))
	receiver.WriteRTP(h264Packet(8, NALUTypePPS))
	receiver.WriteRTP(h264Packet(9, NALUTypeIFrame))

	var got2 []uint16
	done2 := make(chan struct{})

	sender2 := core.NewSender(media, codec)
	sender2.Handler = func(packet *rtp.Packet) {
		got2 = append(got2, packet.SequenceNumber)
		if len(got2) == 3 {
			close(done2)
		}
	}
	sender2.HandleRTP(receiver)

	<-done2
	require.Equal(t, []uint16{7, 8, 9}, got2)
}

func TestIsKeyframeRTP(t *testing.T) {
	require.True(t, IsKeyframeRTP([]byte{0x65, 0x00}))  // IDR
	require.True(t, IsKeyframeRTP([]byte{0x67, 0x00}))  // SPS
	require.True(t, IsKeyframeRTP([]byte{0x7C, 0x85}))  // FU-A start of IDR
	require.False(t, IsKeyframeRTP([]byte{0x7C, 0x05})) // FU-A middle of IDR
	require.False(t, IsKeyframeRTP([]byte{0x61, 0x00})) // P-frame
	require.False(t, IsKeyframeRTP([]byte{0x7C, 0x81})) // FU-A start of P
}
