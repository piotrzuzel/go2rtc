package homekit

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/AlexxIT/go2rtc/pkg/hap"
	"github.com/AlexxIT/go2rtc/pkg/hap/tlv8"
)

type HandlerFunc func(net.Conn) error

type Server interface {
	ServerPair
	ServerAccessory
}

type ServerPair interface {
	GetPair(id string) []byte
	AddPair(id string, public []byte, permissions byte)
	DelPair(id string)
}

type ServerAccessory interface {
	GetAccessories(conn net.Conn) []*hap.Accessory
	GetCharacteristic(conn net.Conn, aid uint8, iid uint64) any
	SetCharacteristic(conn net.Conn, aid uint8, iid uint64, value any)
	GetImage(conn net.Conn, width, height int) []byte
}

func ServerHandler(server Server) HandlerFunc {
	cleanup := func(events *eventWriter) {
		unsubscribeAll(server, nil, events)
	}
	return handleRequest(func(conn net.Conn, req *http.Request, events *eventWriter) (*http.Response, error) {
		switch req.URL.Path {
		case hap.PathPairings:
			return handlePairings(req, server)

		case hap.PathAccessories:
			body := hap.JSONAccessories{Value: server.GetAccessories(conn)}
			return makeResponse(hap.MimeJSON, body)

		case hap.PathCharacteristics:
			switch req.Method {
			case "GET":
				var v hap.JSONCharacters

				id := req.URL.Query().Get("id")
				for _, id = range strings.Split(id, ",") {
					s1, s2, _ := strings.Cut(id, ".")
					aid, _ := strconv.Atoi(s1)
					iid, _ := strconv.ParseUint(s2, 10, 64)
					val := server.GetCharacteristic(conn, uint8(aid), iid)

					v.Value = append(v.Value, hap.JSONCharacter{AID: uint8(aid), IID: iid, Value: val})
				}

				return makeResponse(hap.MimeJSON, v)

			case "PUT":
				var v struct {
					Value []struct {
						AID   uint8  `json:"aid"`
						IID   uint64 `json:"iid"`
						Value any    `json:"value"`
						Event *bool  `json:"ev"`
					} `json:"characteristics"`
				}
				if err := json.NewDecoder(req.Body).Decode(&v); err != nil {
					return nil, err
				}

				for _, char := range v.Value {
					// event subscription (ev) and value write may come separately
					if char.Event != nil {
						subscribe(server, conn, char.AID, char.IID, *char.Event, events)
					}
					if char.Value != nil {
						server.SetCharacteristic(conn, char.AID, char.IID, char.Value)
					}
				}

				res := &http.Response{
					StatusCode: http.StatusNoContent,
					Proto:      "HTTP",
					ProtoMajor: 1,
					ProtoMinor: 1,
				}
				return res, nil
			}

		case hap.PathResource:
			var v struct {
				Width  int    `json:"image-width"`
				Height int    `json:"image-height"`
				Type   string `json:"resource-type"`
			}
			if err := json.NewDecoder(req.Body).Decode(&v); err != nil {
				return nil, err
			}

			body := server.GetImage(conn, v.Width, v.Height)
			return makeResponse("image/jpeg", body)
		}

		return nil, errors.New("hap: unsupported path: " + req.RequestURI)
	}, cleanup)
}

// eventWriter delivers EVENT/1.0 frames to a connection, serialized
// with regular HTTP responses so frames never interleave
type eventWriter struct {
	conn net.Conn
	mu   sync.Mutex
}

func (e *eventWriter) Write(p []byte) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.conn.Write(p)
}

// subscribe adds or removes the connection as characteristic event listener
func subscribe(server Server, conn net.Conn, aid uint8, iid uint64, enable bool, events *eventWriter) {
	for _, acc := range server.GetAccessories(conn) {
		if acc.AID != aid {
			continue
		}
		if char := acc.GetCharacterByID(iid); char != nil {
			if enable {
				char.AddListener(events)
			} else {
				char.RemoveListener(events)
			}
		}
	}
}

// unsubscribeAll removes the connection from every characteristic
// (connection closed)
func unsubscribeAll(server Server, conn net.Conn, events *eventWriter) {
	for _, acc := range server.GetAccessories(conn) {
		for _, srv := range acc.Services {
			for _, char := range srv.Characters {
				char.RemoveListener(events)
			}
		}
	}
}

func handleRequest(handle func(conn net.Conn, req *http.Request, events *eventWriter) (*http.Response, error), cleanup func(events *eventWriter)) HandlerFunc {
	return func(conn net.Conn) error {
		rw := bufio.NewReaderSize(conn, 16*1024)
		wr := bufio.NewWriterSize(conn, 16*1024)

		events := &eventWriter{conn: conn}
		if cleanup != nil {
			defer cleanup(events)
		}

		for {
			req, err := http.ReadRequest(rw)
			//debug(req)
			if err != nil {
				return err
			}

			res, err := handle(conn, req, events)
			//debug(res)
			if err != nil {
				return err
			}

			events.mu.Lock()
			if err = res.Write(wr); err == nil {
				err = wr.Flush()
			}
			events.mu.Unlock()

			if err != nil {
				return err
			}
		}
	}
}

func handlePairings(req *http.Request, srv ServerPair) (*http.Response, error) {
	cmd := struct {
		Method      byte   `tlv8:"0"`
		Identifier  string `tlv8:"1"`
		PublicKey   string `tlv8:"3"`
		State       byte   `tlv8:"6"`
		Permissions byte   `tlv8:"11"`
	}{}

	if err := tlv8.UnmarshalReader(req.Body, req.ContentLength, &cmd); err != nil {
		return nil, err
	}

	switch cmd.Method {
	case 3: // add
		srv.AddPair(cmd.Identifier, []byte(cmd.PublicKey), cmd.Permissions)
	case 4: // delete
		srv.DelPair(cmd.Identifier)
	}

	body := struct {
		State byte `tlv8:"6"`
	}{
		State: hap.StateM2,
	}

	return makeResponse(hap.MimeTLV8, body)
}

func makeResponse(mime string, v any) (*http.Response, error) {
	var body []byte
	var err error

	switch mime {
	case hap.MimeJSON:
		body, err = json.Marshal(v)
	case hap.MimeTLV8:
		body, err = tlv8.Marshal(v)
	case "image/jpeg":
		body = v.([]byte)
	}

	if err != nil {
		return nil, err
	}

	res := &http.Response{
		StatusCode: http.StatusOK,
		Proto:      "HTTP",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header: http.Header{
			"Content-Type":   []string{mime},
			"Content-Length": []string{strconv.Itoa(len(body))},
		},
		ContentLength: int64(len(body)),
		Body:          io.NopCloser(bytes.NewReader(body)),
	}
	return res, nil
}
