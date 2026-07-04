package mdns

import (
	"net"

	"github.com/miekg/dns"
)

// ClassCacheFlush https://datatracker.ietf.org/doc/html/rfc6762#section-10.2
const ClassCacheFlush = 0x8001

func Serve(service string, entries []*ServiceEntry) error {
	b := Browser{Service: service}

	if err := b.ListenMulticastUDP(); err != nil {
		return err
	}

	return b.Serve(entries)
}

func (b *Browser) Serve(entries []*ServiceEntry) error {
	names := make(map[string]*ServiceEntry, len(entries))
	hosts := make(map[string]*ServiceEntry, len(entries))
	for _, entry := range entries {
		name := entry.name() + "." + b.Service
		names[name] = entry
		hosts[entry.name()+".local."] = entry
	}

	done := make(chan struct{})
	defer close(done)

	packets := b.readPackets(b.conns(), done)

	for pkt := range packets {
		var req dns.Msg // request
		if err := req.Unpack(pkt.data); err != nil {
			continue
		}

		// skip messages without Questions
		if req.Question == nil {
			continue
		}

		remoteIP := pkt.addr.(*net.UDPAddr).IP
		localIP := b.MatchLocalIP(remoteIP)

		// skip messages from unknown networks (can be docker network)
		if localIP == nil {
			continue
		}

		var unicast bool

		var res dns.Msg // response
		for _, q := range req.Question {
			// support QU questions (unicast response bit in Qclass)
			// https://datatracker.ietf.org/doc/html/rfc6762#section-5.4
			if q.Qclass&0x7FFF != dns.ClassINET {
				continue
			}

			switch q.Qtype {
			case dns.TypePTR:
				if q.Name == ServiceDNSSD {
					AppendDNSSD(&res, b.Service)
				} else if q.Name == b.Service {
					for _, entry := range entries {
						AppendEntry(&res, entry, b.Service, localIP)
					}
				} else if entry, ok := names[q.Name]; ok {
					AppendEntry(&res, entry, b.Service, localIP)
				} else {
					continue
				}

			case dns.TypeTXT:
				entry, ok := names[q.Name]
				if !ok {
					continue
				}
				res.Answer = append(res.Answer, newTXT(entry, q.Name))

			case dns.TypeSRV:
				entry, ok := names[q.Name]
				if !ok {
					continue
				}
				res.Answer = append(res.Answer, newSRV(entry, q.Name))
				res.Extra = append(res.Extra, newA(entry, localIP))

			case dns.TypeA, dns.TypeANY:
				entry, ok := hosts[q.Name]
				if !ok {
					continue
				}
				res.Answer = append(res.Answer, newA(entry, localIP))
				res.Extra = append(res.Extra, newNSEC(entry))

			case dns.TypeAAAA:
				entry, ok := hosts[q.Name]
				if !ok {
					continue
				}
				// negative response per RFC 6762 section 6.1 - without it
				// Apple controllers wait out the AAAA timeout on every connect
				res.Answer = append(res.Answer, newNSEC(entry))

			default:
				continue
			}

			unicast = unicast || q.Qclass&0x8000 != 0
		}

		if res.Answer == nil {
			continue
		}

		res.MsgHdr.Response = true
		res.MsgHdr.Authoritative = true

		data, err := res.Pack()
		if err != nil {
			continue
		}

		if unicast {
			// reply directly to the asker
			for _, send := range b.Sends {
				if _, err = send.WriteTo(data, pkt.addr); err == nil {
					break
				}
			}
		} else {
			for _, send := range b.Sends {
				_, _ = send.WriteTo(data, MulticastAddr)
			}
		}
	}

	return nil
}

func (b *Browser) MatchLocalIP(remote net.IP) net.IP {
	for _, ipn := range b.Nets {
		if ipn.Contains(remote) {
			return ipn.IP
		}
	}
	return nil
}

func AppendDNSSD(msg *dns.Msg, service string) {
	msg.Answer = append(
		msg.Answer,
		&dns.PTR{
			Hdr: dns.RR_Header{
				Name:   ServiceDNSSD,  // _services._dns-sd._udp.local.
				Rrtype: dns.TypePTR,   // 12
				Class:  dns.ClassINET, // 1
				Ttl:    4500,
			},
			Ptr: service, // _home-assistant._tcp.local.
		},
	)
}

func AppendEntry(msg *dns.Msg, entry *ServiceEntry, service string, ip net.IP) {
	ptrName := entry.name() + "." + service

	msg.Answer = append(
		msg.Answer,
		&dns.PTR{
			Hdr: dns.RR_Header{
				Name:   service,       // _home-assistant._tcp.local.
				Rrtype: dns.TypePTR,   // 12
				Class:  dns.ClassINET, // 1
				Ttl:    4500,
			},
			Ptr: ptrName, // Home\ Assistant._home-assistant._tcp.local.
		},
	)
	msg.Extra = append(
		msg.Extra,
		newTXT(entry, ptrName),
		newSRV(entry, ptrName),
		newA(entry, ip),
		newNSEC(entry),
	)
}

// newNSEC - assert that only the A record exists for the host,
// so clients don't wait for an AAAA answer that will never come
// https://datatracker.ietf.org/doc/html/rfc6762#section-6.1
func newNSEC(entry *ServiceEntry) *dns.NSEC {
	name := entry.name() + ".local."
	return &dns.NSEC{
		Hdr: dns.RR_Header{
			Name:   name,
			Rrtype: dns.TypeNSEC,
			Class:  ClassCacheFlush,
			Ttl:    120,
		},
		NextDomain: name,
		TypeBitMap: []uint16{dns.TypeA},
	}
}

func newTXT(entry *ServiceEntry, ptrName string) *dns.TXT {
	return &dns.TXT{
		Hdr: dns.RR_Header{
			Name:   ptrName,         // Home\ Assistant._home-assistant._tcp.local.
			Rrtype: dns.TypeTXT,     // 16
			Class:  ClassCacheFlush, // 32769
			Ttl:    4500,
		},
		Txt: entry.TXT(),
	}
}

func newSRV(entry *ServiceEntry, ptrName string) *dns.SRV {
	return &dns.SRV{
		Hdr: dns.RR_Header{
			Name:   ptrName,         // Home\ Assistant._home-assistant._tcp.local.
			Rrtype: dns.TypeSRV,     // 33
			Class:  ClassCacheFlush, // 32769
			Ttl:    120,
		},
		Port:   entry.Port,               // 8123
		Target: entry.name() + ".local.", // 963f1fa82b7142809711cebe7c826322.local.
	}
}

func newA(entry *ServiceEntry, ip net.IP) *dns.A {
	return &dns.A{
		Hdr: dns.RR_Header{
			Name:   entry.name() + ".local.", // 963f1fa82b7142809711cebe7c826322.local.
			Rrtype: dns.TypeA,                // 1
			Class:  ClassCacheFlush,          // 32769
			Ttl:    120,
		},
		A: ip,
	}
}
