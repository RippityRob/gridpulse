// GridPulse collector — Step 2b: concurrent TCP listener that decodes
// sensor packets and persists them to PostgreSQL in batches.
//
// Architecture:
//
//	sensor conns --> handleConn (goroutine each) --> readings channel
//	                                                     |
//	                                            writer goroutine
//	                                     (batch by size or time, COPY to db)
//
// Decoupling network handling from persistence via a channel means a slow
// database never blocks packet reception, and inserts happen in efficient
// batches instead of one round-trip per reading.
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"

	"database/sql"

	"github.com/lib/pq"
)

const (
	packetSize = 32
	magic      = 0x4750
	protoVer   = 1
	flagFault  = 0x01

	batchSize     = 100                    // flush when this many readings buffered
	batchInterval = 500 * time.Millisecond // ...or at least this often
	channelDepth  = 4096                   // absorb db hiccups without blocking readers
)

// Reading is one decoded sensor measurement.
type Reading struct {
	SensorID  uint16
	Sequence  uint32
	Timestamp time.Time
	Voltage   float64 // volts
	Current   float64 // amps
	Frequency float64 // hertz
	Fault     bool
}

// crc16CCITT implements CRC-16/CCITT-FALSE (poly 0x1021, init 0xFFFF),
// matching the sensor firmware.
func crc16CCITT(data []byte) uint16 {
	crc := uint16(0xFFFF)
	for _, b := range data {
		crc ^= uint16(b) << 8
		for i := 0; i < 8; i++ {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

// decodePacket parses and validates one 32-byte packet.
func decodePacket(buf []byte) (Reading, error) {
	if len(buf) != packetSize {
		return Reading{}, fmt.Errorf("bad packet size %d", len(buf))
	}
	if binary.BigEndian.Uint16(buf[0:2]) != magic {
		return Reading{}, errors.New("bad magic")
	}
	if buf[2] != protoVer {
		return Reading{}, fmt.Errorf("unsupported protocol version %d", buf[2])
	}
	if binary.BigEndian.Uint16(buf[30:32]) != crc16CCITT(buf[:30]) {
		return Reading{}, errors.New("crc mismatch")
	}
	tsMillis := binary.BigEndian.Uint64(buf[10:18])
	return Reading{
		SensorID:  binary.BigEndian.Uint16(buf[4:6]),
		Sequence:  binary.BigEndian.Uint32(buf[6:10]),
		Timestamp: time.UnixMilli(int64(tsMillis)).UTC(),
		Voltage:   float64(binary.BigEndian.Uint32(buf[18:22])) / 1000.0,
		Current:   float64(binary.BigEndian.Uint32(buf[22:26])) / 1000.0,
		Frequency: float64(binary.BigEndian.Uint32(buf[26:30])) / 1000.0,
		Fault:     buf[3]&flagFault != 0,
	}, nil
}

// handleConn owns one sensor TCP stream, pushing decoded readings into out.
func handleConn(conn net.Conn, out chan<- Reading) {
	defer conn.Close()
	peer := conn.RemoteAddr().String()
	log.Printf("stream connected: %s", peer)

	buf := make([]byte, packetSize)
	var good, bad int
	for {
		if _, err := io.ReadFull(conn, buf); err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf("stream %s read error: %v", peer, err)
			}
			break
		}
		r, err := decodePacket(buf)
		if err != nil {
			bad++
			log.Printf("stream %s: dropping packet: %v", peer, err)
			continue
		}
		good++
		select {
		case out <- r:
		default:
			// Channel full: db is badly behind. Drop rather than stall
			// the network reader; the drop is visible in the logs.
			log.Printf("stream %s: buffer full, dropping reading", peer)
		}
	}
	log.Printf("stream closed: %s (%d valid, %d dropped)", peer, good, bad)
}

// writer drains the readings channel and copies batches into Postgres
// using the COPY protocol (one bulk transfer per batch instead of one
// round-trip per row).
func writer(ctx context.Context, db *sql.DB, in <-chan Reading) {
	batch := make([]Reading, 0, batchSize)
	ticker := time.NewTicker(batchInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		err := func() error {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			stmt, err := tx.Prepare(pq.CopyIn("readings",
				"time", "sensor_id", "sequence",
				"voltage_v", "current_a", "frequency_hz", "fault"))
			if err != nil {
				return err
			}
			for _, r := range batch {
				if _, err := stmt.Exec(r.Timestamp, int32(r.SensorID),
					int64(r.Sequence), r.Voltage, r.Current,
					r.Frequency, r.Fault); err != nil {
					return err
				}
			}
			if _, err := stmt.Exec(); err != nil { // final Exec flushes the COPY
				return err
			}
			if err := stmt.Close(); err != nil {
				return err
			}
			return tx.Commit()
		}()
		if err != nil {
			log.Printf("db write failed (%d readings lost): %v", len(batch), err)
		} else {
			log.Printf("wrote %d readings", len(batch))
		}
		batch = batch[:0]
	}

	for {
		select {
		case r, ok := <-in:
			if !ok {
				flush()
				return
			}
			batch = append(batch, r)
			if len(batch) >= batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			flush()
			return
		}
	}
}

func main() {
	listenAddr := flag.String("listen", ":9000", "address to accept sensor streams on")
	dbURL := flag.String("db", envOr("DATABASE_URL",
		"postgres://gridpulse:gridpulse@localhost:5432/gridpulse?sslmode=disable"),
		"postgres connection url")
	flag.Parse()

	ctx := context.Background()
	db, err := sql.Open("postgres", *dbURL)
	if err != nil {
		log.Fatalf("db config: %v", err)
	}
	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("db unreachable: %v", err)
	}
	log.Printf("connected to postgres")

	readings := make(chan Reading, channelDepth)
	go writer(ctx, db, readings)

	ln, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("gridpulse-collector listening on %s", *listenAddr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		go handleConn(conn, readings)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
