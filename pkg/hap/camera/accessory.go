package camera

import (
	"github.com/AlexxIT/go2rtc/pkg/hap"
	"github.com/AlexxIT/go2rtc/pkg/hap/tlv8"
)

func NewAccessory(manuf, model, name, serial, firmware string) *hap.Accessory {
	acc := &hap.Accessory{
		AID: hap.DeviceAID,
		Services: []*hap.Service{
			hap.ServiceAccessoryInformation(manuf, model, name, serial, firmware),
			ServiceCameraRTPStreamManagement(),
			hap.ServiceHAPProtocolInformation(),
			ServiceMicrophone(),
			ServiceCameraOperatingMode(),
			ServiceMotionSensor(name),
		},
	}
	acc.InitIID()
	return acc
}

// ServiceMotionSensor - Apple controllers keep a persistent connection
// to accessories with subscribable sensors, which makes every stream
// start much faster (no reconnect + pair-verify on each interaction)
func ServiceMotionSensor(name string) *hap.Service {
	return &hap.Service{
		Type: "85", // 'MotionSensor'
		Characters: []*hap.Character{
			{
				Type:   "23",
				Format: hap.FormatString,
				Value:  name + " Motion",
				Perms:  hap.PR,
				//Descr:  "Name"
			},
			{
				Type:   "22",
				Format: hap.FormatBool,
				Value:  false,
				Perms:  hap.EVPR,
				//Descr:  "Motion Detected"
			},
			{
				Type:   "75",
				Format: hap.FormatBool,
				Value:  true,
				Perms:  hap.EVPR,
				//Descr:  "Status Active"
			},
		},
	}
}

// ServiceCameraOperatingMode - iOS reads and writes these on camera
// settings, real cameras always expose them
func ServiceCameraOperatingMode() *hap.Service {
	return &hap.Service{
		Type: "21A", // 'CameraOperatingMode'
		Characters: []*hap.Character{
			{
				Type:   "21B",
				Format: hap.FormatBool,
				Value:  true,
				Perms:  hap.EVPRPW,
				//Descr:  "HomeKit Camera Active"
			},
			{
				Type:   "223",
				Format: hap.FormatBool,
				Value:  true,
				Perms:  hap.EVPRPW,
				//Descr:  "Event Snapshots Active"
			},
			{
				Type:   "225",
				Format: hap.FormatBool,
				Value:  true,
				Perms:  hap.EVPRPW,
				//Descr:  "Periodic Snapshots Active"
			},
		},
	}
}


func ServiceMicrophone() *hap.Service {
	return &hap.Service{
		Type: "112", // 'Microphone'
		Characters: []*hap.Character{
			{
				Type:   "11A",
				Format: hap.FormatBool,
				Value:  0,
				Perms:  hap.EVPRPW,
				//Descr:  "Mute",
			},
			//{
			//	Type:   "119",
			//	Format: hap.FormatUInt8,
			//	Value:  100,
			//	Perms:  hap.EVPRPW,
			//	//Descr:    "Volume",
			//	//Unit:     hap.UnitPercentage,
			//	//MinValue: 0,
			//	//MaxValue: 100,
			//	//MinStep:  1,
			//},
		},
	}
}

func ServiceCameraRTPStreamManagement() *hap.Service {
	val120, _ := tlv8.MarshalBase64(StreamingStatus{
		Status: StreamingStatusAvailable,
	})
	val114, _ := tlv8.MarshalBase64(SupportedVideoStreamConfiguration{
		Codecs: []VideoCodecConfiguration{
			{
				CodecType: VideoCodecTypeH264,
				CodecParams: []VideoCodecParameters{
					{
						ProfileID: []byte{VideoCodecProfileMain},
						Level:     []byte{VideoCodecLevel31, VideoCodecLevel40},
					},
				},
				VideoAttrs: []VideoCodecAttributes{
					{Width: 1920, Height: 1080, Framerate: 30},
					{Width: 1280, Height: 720, Framerate: 30}, // important for iPhones
					{Width: 320, Height: 240, Framerate: 15},  // apple watch
				},
			},
		},
	})
	val115, _ := tlv8.MarshalBase64(SupportedAudioStreamConfiguration{
		Codecs: []AudioCodecConfiguration{
			{
				CodecType: AudioCodecTypeOpus,
				CodecParams: []AudioCodecParameters{
					{
						Channels:    1,
						BitrateMode: AudioCodecBitrateVariable,
						SampleRate:  []byte{AudioCodecSampleRate16Khz},
					},
				},
			},
		},
		ComfortNoiseSupport: 0,
	})
	val116, _ := tlv8.MarshalBase64(SupportedRTPConfiguration{
		SRTPCryptoType: []byte{CryptoAES_CM_128_HMAC_SHA1_80},
	})

	service := &hap.Service{
		Type: "110", // 'CameraRTPStreamManagement'
		Characters: []*hap.Character{
			{
				Type:   TypeStreamingStatus,
				Format: hap.FormatTLV8,
				Value:  val120,
				Perms:  hap.EVPR,
				//Descr:  "Streaming Status",
			},
			{
				Type:   TypeSupportedVideoStreamConfiguration,
				Format: hap.FormatTLV8,
				Value:  val114,
				Perms:  hap.PR,
				//Descr:  "Supported Video Stream Configuration",
			},
			{
				Type:   TypeSupportedAudioStreamConfiguration,
				Format: hap.FormatTLV8,
				Value:  val115,
				Perms:  hap.PR,
				//Descr:  "Supported Audio Stream Configuration",
			},
			{
				Type:   TypeSupportedRTPConfiguration,
				Format: hap.FormatTLV8,
				Value:  val116,
				Perms:  hap.PR,
				//Descr:  "Supported RTP Configuration",
			},
			{
				Type:   "B0",
				Format: hap.FormatUInt8,
				Value:  1,
				Perms:  hap.EVPRPW,
				//Descr:    "Active",
				//MinValue: 0,
				//MaxValue: 1,
				//MinStep:  1,
				//ValidVal: []any{0, 1},
			},
			{
				Type:   TypeSelectedStreamConfiguration,
				Format: hap.FormatTLV8,
				Value:  "", // important empty
				Perms:  hap.PRPW,
				//Descr:  "Selected RTP Stream Configuration",
			},
			{
				Type:   TypeSetupEndpoints,
				Format: hap.FormatTLV8,
				Value:  "", // important empty
				Perms:  hap.PRPW,
				//Descr:  "Setup Endpoints",
			},
		},
	}

	return service
}
