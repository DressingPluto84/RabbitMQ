package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// AMQP wire-level encoding — the tedious byte layer (§4.2.3–§4.2.5).
// Everything here is big-endian / network byte order. Nothing in this file
// knows anything about the handshake; it just turns values into bytes.

// writeShortStr: 1-byte length + bytes (max 255). (§4.2.5.3)
func writeShortStr(buf *bytes.Buffer, s string) {
	buf.WriteByte(byte(len(s)))
	buf.WriteString(s)
}

func readShortStr(frame []byte, offset int) (string, int) {
	length := frame[offset]
	return string(frame[offset + 1:offset + int(length) + 1]), offset + int(length) + 1
}

// writeLongStr: 4-byte length + bytes. (§4.2.5.3)
func writeLongStr(buf *bytes.Buffer, s string) {
	binary.Write(buf, binary.BigEndian, uint32(len(s)))
	buf.WriteString(s)
}

// writeEmptyTable: a field-table with no entries is just a zero length. (§4.2.5.5)
func writeEmptyTable(buf *bytes.Buffer) {
	binary.Write(buf, binary.BigEndian, uint32(0))
}

// methodPayload wraps method arguments with their class-id + method-id header,
// producing the payload of a METHOD frame. (§4.2.4)
func methodPayload(classID, methodID uint16, args []byte) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, classID)
	binary.Write(buf, binary.BigEndian, methodID)
	buf.Write(args)
	return buf.Bytes()
}

// writeFrame emits one complete frame: 7-byte header + payload + frame-end. (§4.2.3)
//
//	+------+---------+---------+ +-------------+ +-----------+
//	| type | channel |  size   | |   payload   | | frame-end |
//	+------+---------+---------+ +-------------+ +-----------+
//	 octet   short      long        size octets     0xCE
func writeFrame(w io.Writer, frameType byte, channel uint16, payload []byte) error {
	var hdr [7]byte
	hdr[0] = frameType
	binary.BigEndian.PutUint16(hdr[1:3], channel)
	binary.BigEndian.PutUint32(hdr[3:7], uint32(len(payload)))

	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	_, err := w.Write([]byte{frameEnd})
	return err
}

// readFrame reads one complete frame and returns its type, channel, and payload.
// It's the mirror of writeFrame: read the 7-byte header, read `size` payload
// bytes, then verify the 0xCE frame-end. (§4.2.3)
func readFrame(r io.Reader) (frameType byte, channel uint16, payload []byte, err error) {
	var hdr [7]byte
	if _, err = io.ReadFull(r, hdr[:]); err != nil {
		return
	}
	frameType = hdr[0]
	channel = binary.BigEndian.Uint16(hdr[1:3])
	size := binary.BigEndian.Uint32(hdr[3:7])

	payload = make([]byte, size)
	if _, err = io.ReadFull(r, payload); err != nil {
		return
	}

	var end [1]byte
	if _, err = io.ReadFull(r, end[:]); err != nil {
		return
	}
	if end[0] != frameEnd {
		err = fmt.Errorf("bad frame-end: got 0x%02X, want 0xCE", end[0])
	}
	return
}
