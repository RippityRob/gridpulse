# GridPulse

A distributed grid telemetry pipeline: simulated field sensors stream power
readings over a compact binary protocol into a concurrent collector, backed by
time-series storage and anomaly analytics. Built as a scaled-down model of
real grid monitoring infrastructure.

## Architecture

```
[C sensor simulator] --TCP/binary--> [Go collector] --> [PostgreSQL]
      (fleet of                        (goroutine            |
   emulated devices)                  per stream)            v
                                                   [Python analytics API]
                                                            |
                                                            v
                                                     [Web dashboard]
```

| Component        | Language | Status  |
|------------------|----------|---------|
| Sensor simulator | C        | Done    |
| Collector        | Go       | Planned |
| Storage schema   | SQL      | Planned |
| Analytics API    | Python   | Planned |
| Dashboard        | React    | Planned |
| Orchestration    | Docker Compose | Planned |
| Deployment       | AWS EC2  | Planned |

## Sensor simulator

Emulates N sensors on an 11 kV feeder emitting voltage/current/frequency at a
configurable rate, with random voltage-sag faults (dip to ~70% nominal,
current spike, frequency wobble). Auto-reconnects with backoff if the
collector goes away.

```
cd sensor-sim && make
./sensor --host 127.0.0.1 --port 9000 --sensors 8 --rate 2 --fault-prob 0.005
```

### Wire format

32-byte packets, big-endian, CRC-16/CCITT-FALSE over the first 30 bytes:

| Offset | Size | Field        | Notes                          |
|--------|------|--------------|--------------------------------|
| 0      | 2    | magic        | 0x4750 ("GP")                  |
| 2      | 1    | version      | 1                              |
| 3      | 1    | flags        | bit0 = fault active            |
| 4      | 2    | sensor_id    |                                |
| 6      | 4    | sequence     | per-sensor counter             |
| 10     | 8    | timestamp_ms | unix epoch ms                  |
| 18     | 4    | voltage_mv   | scaled integer (millivolts)    |
| 22     | 4    | current_ma   | milliamps                      |
| 26     | 4    | freq_mhz     | millihertz (50 Hz = 50000)     |
| 30     | 2    | crc16        |                                |

Scaled integers rather than floats keep the format portable and
independent of IEEE-754 representation.

### Protocol debugging

`tools/decode.py` listens on a port, validates CRCs, and pretty-prints
incoming packets — an executable spec of the wire format.

```
python3 tools/decode.py 9000
```
