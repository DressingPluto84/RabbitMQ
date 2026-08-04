package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"net"
)

// Wire constants.
const (
	frameMethod 	= 1    // METHOD frame type (§4.2.3)
	frameEnd    	= 0xCE // mandatory frame terminator (§4.2.3)

	classConnection = 10   // connection class-id
	methodStart     = 10   // connection.start method-id
	methodStartOk   = 11   // connection.start-ok method-id
	methodTune 		= 30   // connection.tune
	methodTuneOk    = 31   // connection.tune-ok
	methodOpen      = 40   // connection.open
	methodOpenOk    = 41   // connection.open-ok 

	channel_max 	= 0    // max num channels on a connection
	frame_size 		= 4096 // max bytes between frames
	heartbeat		= 0    // how many seconds between heartbeat calls
)

// The 8-byte header a client must send first: "AMQP" 0 0 9 1. (§4.2.2)
var protocolHeader = []byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}

func main() {
	ln, err := net.Listen("tcp", ":5672")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Println("broker listening on :5672")

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		go handleConn(conn)
	}
}

// handleConn drives the connection state machine. Right now it covers the first
// two handshake steps: validate the protocol header, then send connection.start.
func handleConn(conn net.Conn) {
	defer conn.Close()
	log.Printf("client connected: %s", conn.RemoteAddr())

	// Step 1 — read + validate the protocol header (§4.2.2).
	// If it's wrong, the spec says: write our own header back, then close.
	hdr := make([]byte, 8)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		log.Printf("read header: %v", err)
		return
	}
	if !bytes.Equal(hdr, protocolHeader) {
		log.Printf("bad protocol header: % x", hdr)
		conn.Write(protocolHeader)
		return
	}
	log.Printf("valid protocol header received")

	// Step 2 — send connection.start (§2.2.4).
	if err := sendConnectionStart(conn); err != nil {
		log.Printf("send connection.start: %v", err)
		return
	}
	log.Printf("sent connection.start")

	// Step 3 — read connection.start-ok (§2.2.4).
	// We accept but do NOT validate: the client's creds live in the `response`
	// field, which we deliberately ignore. We only confirm it's method 10/11.
	_, _, payload, err := readFrame(conn)
	if err != nil {
		log.Printf("read start-ok: %v", err)
		return
	}
	classID := binary.BigEndian.Uint16(payload[0:2])
	methodID := binary.BigEndian.Uint16(payload[2:4])
	if classID != classConnection || methodID != methodStartOk {
		log.Printf("expected connection.start-ok (10/11), got %d/%d", classID, methodID)
		return
	}
	log.Printf("received connection.start-ok")

	// Step 4 — send connection.tune.
	if err := sendConnectionTune(conn); err != nil {
		log.Printf("tune connection failed: %v", err)
		return
	}
	log.Printf("sent connection.tune")

	// Step 5 — read connection.tune-ok (§2.2.4).
	// Client echoes the limits it agreed to (channel-max, frame-max, heartbeat);
	// we accept whatever it sends and don't enforce it.
	_, _, payload, err = readFrame(conn)
	if err != nil {
		log.Printf("read tune-ok: %v", err)
		return
	}
	if binary.BigEndian.Uint16(payload[0:2]) != classConnection ||
		binary.BigEndian.Uint16(payload[2:4]) != methodTuneOk {
		log.Printf("expected connection.tune-ok (10/31), got %d/%d",
			binary.BigEndian.Uint16(payload[0:2]), binary.BigEndian.Uint16(payload[2:4]))
		return
	}
	log.Printf("received connection.tune-ok")

	// Step 6 — read connection.open (§2.2.4).
	// Fields: virtual-host (shortstr), reserved-1 (shortstr), reserved-2 (bit).
	// We accept any vhost, so just confirm it's 10/40 and ignore the fields.
	_, _, payload, err = readFrame(conn)
	if err != nil {
		log.Printf("read open: %v", err)
		return
	}
	if binary.BigEndian.Uint16(payload[0:2]) != classConnection ||
		binary.BigEndian.Uint16(payload[2:4]) != methodOpen {
		log.Printf("expected connection.open (10/40), got %d/%d",
			binary.BigEndian.Uint16(payload[0:2]), binary.BigEndian.Uint16(payload[2:4]))
		return
	}
	log.Printf("received connection.open")

	// Step 7 — send connection.open-ok (class 10, method 41).
	// One field: reserved-1 (shortstr) — send an empty short string.
	// After this, amqp.Dial returns.
	if err := sendConnectionOpenOk(conn); err != nil {
		log.Printf("open-ok connection failed: %v", err)
		return
	}
	log.Printf("sent connection.open-ok")
}

// sendConnectionStart builds and writes the connection.start method.
// Fields, in order (from the AMQP XML spec):
//
//	version-major     octet             -> 0   (AMQP 0-9-1)
//	version-minor     octet             -> 9
//	server-properties peer-properties   -> {} (empty for now)
//	mechanisms        longstr           -> "PLAIN"
//	locales           longstr           -> "en_US"
func sendConnectionStart(conn net.Conn) error {
	args := new(bytes.Buffer)
	args.WriteByte(0)            // version-major
	args.WriteByte(9)            // version-minor
	writeEmptyTable(args)        // server-properties
	writeLongStr(args, "PLAIN")  // mechanisms
	writeLongStr(args, "en_US")  // locales

	payload := methodPayload(classConnection, methodStart, args.Bytes())
	return writeFrame(conn, frameMethod, 0, payload) // channel 0 = connection-level
}

func sendConnectionTune(conn net.Conn) error {
	args := new(bytes.Buffer)
	binary.Write(args, binary.BigEndian, uint16(channel_max))
	binary.Write(args, binary.BigEndian, uint32(frame_size))
	binary.Write(args, binary.BigEndian, uint16(heartbeat))

	payload := methodPayload(classConnection, methodTune, args.Bytes())
	return writeFrame(conn, frameMethod, 0, payload)
}

func sendConnectionOpenOk(conn net.Conn) error {
	args := new(bytes.Buffer)
	binary.Write(args, binary.BigEndian, byte(0))

	payload := methodPayload(classConnection, methodOpenOk, args.Bytes())
	return writeFrame(conn, frameMethod, 0, payload)
}