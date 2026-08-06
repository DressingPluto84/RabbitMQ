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
	frameMethod 			= 1    // METHOD frame type (§4.2.3)
	frameHeader				= 2    // content HEADER frame type (§4.2.3)
	frameBody				= 3    // content BODY frame type (§4.2.3)
	frameEnd    			= 0xCE // mandatory frame terminator (§4.2.3)

	classConnection 		= 10   // connection class-id
	methodStart     		= 10   // connection.start method-id
	methodStartOk   		= 11   // connection.start-ok method-id
	methodTune 				= 30   // connection.tune
	methodTuneOk    		= 31   // connection.tune-ok
	methodOpen      		= 40   // connection.open
	methodOpenOk    		= 41   // connection.open-ok 

	classChannel        	= 20   // channel class-id
	methodChannelOpen   	= 10   // channel.open
	methodChannelOpenOk 	= 11   // channel.open-ok

	classExchange			= 40   // exchange class-id
	methodExchangeDeclare	= 10   // exchange.declare
	methodExchangeDeclareOk	= 11   // exchange.declare-ok

	classQueue 				= 50   // queue class-id
	methodQueueDeclare  	= 10   // queue.declare 
	methodQueueDeclareOk 	= 11   // queue.declare-ok
	methodQueueBind			= 20   // queue.bind
	methodQueueBindOk		= 21   // queue.bind-ok
	methodQueueUnbind		= 50   // queue.unbind
	methodQueueUnbindOk		= 51   // queue.unbind-ok
	methodQueueDelete		= 40   // queue.delete
	methodQueueDeleteOk		= 41   // queue.delete.ok

	classBasic				= 60   // basic class-id
	methodBasicConsume		= 20   // basic.consume
	methodBasicConsumeOk	= 21   // basic.consume-ok
	methodBasicPublish		= 40   // basic.publish
	methodBasicDeliver		= 60   // basic.deliver

	channel_max 			= 0    // max num channels on a connection
	frame_size 				= 4096 // max bytes between frames
	heartbeat				= 0    // how many seconds between heartbeat calls
)

// queue registry
var reg = &registry{queues: make(map[string]*queue)}
// exhange registry
var exReg = &exchangeRegistry{exchanges: make(map[string]*exchange)}

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
	c := &connection{
		conn: conn,
	}
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
	if err := sendConnectionStart(c); err != nil {
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
	if err := sendConnectionTune(c); err != nil {
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
	if err := sendConnectionOpenOk(c); err != nil {
		log.Printf("open-ok connection failed: %v", err)
		return
	}
	log.Printf("sent connection.open-ok")

	// Handshake done — the connection is open. From here it's no longer a fixed
	// script: the client sends frames whenever it wants, so we loop, read each
	// frame, and dispatch by class/method. This loop IS the connection state
	// machine.
	for {
		frameType, ch, payload, err := readFrame(conn)
		if err != nil {
			log.Printf("read frame: %v", err)
			return
		}
		// Only METHOD frames carry a class/method. Header/body/heartbeat frames
		// are handled elsewhere (or ignored for now); skip them here so we don't
		// index into a payload that has no class/method.
		if frameType != frameMethod || len(payload) < 4 {
			continue
		}
		classID := binary.BigEndian.Uint16(payload[0:2])
		methodID := binary.BigEndian.Uint16(payload[2:4])

		switch {
		case classID == classChannel && methodID == methodChannelOpen:
			// Client opened a channel (conn.Channel()). Reply on the SAME
			// channel number the client chose — not 0.
			if err := sendChannelOpenOk(c, ch); err != nil {
				log.Printf("send channel.open-ok: %v", err)
				return
			}
			log.Printf("channel %d opened", ch)

		case classID == classExchange && methodID == methodExchangeDeclare:
			if err := handleExchangeDeclare(c, ch, payload); err != nil {
				log.Printf("exchange.declare: %v", err)
				return
			}

		case classID == classQueue && methodID == methodQueueDeclare:
			q, err := sendQueueDeclareOk(c, ch, payload)
			if err != nil {
				log.Printf("send queue.declare: %v", err)
				return
			}
			log.Printf("queue %s opened", q)

		case classID == classQueue && methodID == methodQueueBind:
			if err := handleQueueBind(c, ch, payload); err != nil {
				log.Printf("queue.bind: %v", err)
				return
			}

		case classID == classBasic && methodID == methodBasicPublish:
			q, err := addMessageQueue(c, payload)
			if err != nil {
				log.Printf("basic.publish: %v", err)
				return
			}
			log.Printf("message published to queue %q", q)

		case classID == classBasic && methodID == methodBasicConsume:
			if err := handleBasicConsume(c, ch, payload); err != nil {
				log.Printf("basic.consume: %v", err)
				return
			}

		default:
			log.Printf("unhandled method %d/%d on channel %d", classID, methodID, ch)
		}
	}
}

// sendConnectionStart builds and writes the connection.start method.
// Fields, in order (from the AMQP XML spec):
//
//	version-major     octet             -> 0   (AMQP 0-9-1)
//	version-minor     octet             -> 9
//	server-properties peer-properties   -> {} (empty for now)
//	mechanisms        longstr           -> "PLAIN"
//	locales           longstr           -> "en_US"
func sendConnectionStart(c *connection) error {
	args := new(bytes.Buffer)
	args.WriteByte(0)            // version-major
	args.WriteByte(9)            // version-minor
	writeEmptyTable(args)        // server-properties
	writeLongStr(args, "PLAIN")  // mechanisms
	writeLongStr(args, "en_US")  // locales

	payload := methodPayload(classConnection, methodStart, args.Bytes())
	return c.writeFrame(frameMethod, 0, payload) // channel 0 = connection-level
}

func sendConnectionTune(c *connection) error {
	args := new(bytes.Buffer)
	binary.Write(args, binary.BigEndian, uint16(channel_max))
	binary.Write(args, binary.BigEndian, uint32(frame_size))
	binary.Write(args, binary.BigEndian, uint16(heartbeat))

	payload := methodPayload(classConnection, methodTune, args.Bytes())
	return c.writeFrame(frameMethod, 0, payload)
}

func sendConnectionOpenOk(conn *connection) error {
	args := new(bytes.Buffer)
	binary.Write(args, binary.BigEndian, byte(0))

	payload := methodPayload(classConnection, methodOpenOk, args.Bytes())
	return conn.writeFrame(frameMethod, 0, payload)
}

// sendChannelOpenOk replies to a channel.open. Unlike the connection methods,
// this goes back on the client's channel number, not 0. Its one field
// (reserved-1) is a longstr, so we write an empty longstr (4 zero bytes).
func sendChannelOpenOk(c *connection, channel uint16) error {
	args := new(bytes.Buffer)
	writeLongStr(args, "") // reserved-1

	payload := methodPayload(classChannel, methodChannelOpenOk, args.Bytes())
	return c.writeFrame(frameMethod, channel, payload)
}

func sendQueueDeclareOk(c *connection, channel uint16, payload []byte) (string, error) {
	name, _ := readShortStr(payload, 6) // 2 bytes each for class type and method, 2 bytes for reserved short

	addQueue(reg, name)

	args := new(bytes.Buffer)
	writeShortStr(args, name) // write q name
	binary.Write(args, binary.BigEndian, uint32(0)) // message-count
	binary.Write(args, binary.BigEndian, uint32(0)) // consumer-count

	payload = methodPayload(classQueue, methodQueueDeclareOk, args.Bytes())
	return name, c.writeFrame(frameMethod, channel, payload)
}

func addMessageQueue(c *connection, payload []byte) (string, error) {
	// method header
	_, off := readShortStr(payload, 6)
	routingKey, _ := readShortStr(payload, off)

	// payload header and body — read from our own socket (reads don't need the lock)
	_, _, _, err := readFrame(c.conn)
	if err != nil {
		return "", err
	}
	_, _, body, err := readFrame(c.conn)
	if err != nil {
		return "", err
	}

	m := message{
		payload: body,
		routingKey: routingKey,
	}
	if err := addMessage(reg, routingKey, m); err != nil {
		return "", err
	}
	return routingKey, nil
}