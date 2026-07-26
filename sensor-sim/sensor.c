/*
 * GridPulse sensor simulator
 * --------------------------
 * Emulates a fleet of grid telemetry sensors (think: substation
 * instruments) streaming voltage / current / frequency readings
 * to a collector over TCP, using a compact binary wire format.
 *
 * Wire format (32 bytes, all multi-byte fields big-endian):
 *
 *   offset  size  field
 *   0       2     magic        0x4750 ("GP")
 *   2       1     version      protocol version (1)
 *   3       1     flags        bit0 = fault condition active
 *   4       2     sensor_id
 *   6       4     sequence     per-sensor packet counter
 *   10      8     timestamp_ms unix epoch milliseconds
 *   18      4     voltage_mv   line voltage in millivolts
 *   22      4     current_ma   line current in milliamps
 *   26      4     freq_mhz     frequency in millihertz (50 Hz = 50000)
 *   30      2     crc16        CRC-16/CCITT-FALSE over bytes 0..29
 *
 * Scaled integers (mV/mA/mHz) are used instead of floats so the
 * format has no dependency on IEEE-754 representation and stays
 * trivially portable.
 *
 * Usage:
 *   ./sensor [--host HOST] [--port PORT] [--sensors N] [--rate HZ]
 *            [--fault-prob P]
 *
 * Defaults: 127.0.0.1:9000, 4 sensors, 2 packets/sec each,
 * 0.5% chance per tick that a healthy sensor develops a voltage sag.
 */

#define _POSIX_C_SOURCE 200809L

#include <arpa/inet.h>
#include <errno.h>
#include <math.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define MAGIC        0x4750
#define PROTO_VER    1
#define PACKET_SIZE  32
#define FLAG_FAULT   0x01

/* Nominal values for an 11 kV distribution feeder */
#define NOMINAL_V    11000.0   /* volts  */
#define NOMINAL_I    180.0     /* amps   */
#define NOMINAL_F    50.0      /* hertz  */

typedef struct {
    uint16_t id;
    uint32_t sequence;
    int      fault_ticks_left;  /* >0 while a sag is in progress */
} sensor_t;

static volatile sig_atomic_t running = 1;

static void handle_sigint(int sig) { (void)sig; running = 0; }

/* CRC-16/CCITT-FALSE: poly 0x1021, init 0xFFFF, no reflection */
static uint16_t crc16_ccitt(const uint8_t *data, size_t len)
{
    uint16_t crc = 0xFFFF;
    for (size_t i = 0; i < len; i++) {
        crc ^= (uint16_t)data[i] << 8;
        for (int b = 0; b < 8; b++)
            crc = (crc & 0x8000) ? (uint16_t)((crc << 1) ^ 0x1021)
                                 : (uint16_t)(crc << 1);
    }
    return crc;
}

static uint64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)(ts.tv_nsec / 1000000L);
}

/* Uniform random double in [0, 1) */
static double frand(void) { return (double)rand() / ((double)RAND_MAX + 1.0); }

/* Gaussian-ish noise via sum of uniforms (Irwin-Hall, cheap and fine here) */
static double noise(double scale)
{
    double n = 0.0;
    for (int i = 0; i < 4; i++) n += frand();
    return (n - 2.0) * scale;   /* centred on 0 */
}

static void write_u16(uint8_t *p, uint16_t v) { uint16_t n = htons(v); memcpy(p, &n, 2); }
static void write_u32(uint8_t *p, uint32_t v) { uint32_t n = htonl(v); memcpy(p, &n, 4); }
static void write_u64(uint8_t *p, uint64_t v)
{
    write_u32(p, (uint32_t)(v >> 32));
    write_u32(p + 4, (uint32_t)(v & 0xFFFFFFFFULL));
}

static size_t build_packet(uint8_t *buf, sensor_t *s)
{
    double v = NOMINAL_V + noise(60.0);        /* ~±120 V ripple  */
    double i = NOMINAL_I + noise(6.0);         /* ~±12 A ripple   */
    double f = NOMINAL_F + noise(0.015);       /* ~±30 mHz drift  */
    uint8_t flags = 0;

    if (s->fault_ticks_left > 0) {
        /* Voltage sag: dip to ~70% nominal, current spikes, freq wobbles */
        v *= 0.70 + noise(0.02);
        i *= 1.60 + noise(0.05);
        f += noise(0.08);
        flags |= FLAG_FAULT;
        s->fault_ticks_left--;
    }

    write_u16(buf + 0, MAGIC);
    buf[2] = PROTO_VER;
    buf[3] = flags;
    write_u16(buf + 4, s->id);
    write_u32(buf + 6, s->sequence++);
    write_u64(buf + 10, now_ms());
    write_u32(buf + 18, (uint32_t)llround(v * 1000.0));
    write_u32(buf + 22, (uint32_t)llround(i * 1000.0));
    write_u32(buf + 26, (uint32_t)llround(f * 1000.0));
    write_u16(buf + 30, crc16_ccitt(buf, 30));

    return PACKET_SIZE;
}

static int connect_collector(const char *host, uint16_t port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return -1; }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
        fprintf(stderr, "invalid host address: %s\n", host);
        close(fd);
        return -1;
    }
    if (connect(fd, (struct sockaddr *)&addr, sizeof addr) < 0) {
        perror("connect");
        close(fd);
        return -1;
    }
    return fd;
}

static int send_all(int fd, const uint8_t *buf, size_t len)
{
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(fd, buf + sent, len - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        sent += (size_t)n;
    }
    return 0;
}

int main(int argc, char **argv)
{
    const char *host   = "127.0.0.1";
    uint16_t    port   = 9000;
    int         count  = 4;
    double      rate   = 2.0;     /* packets per second per sensor */
    double      fprob  = 0.005;   /* per-tick fault probability     */

    for (int a = 1; a < argc - 1; a++) {
        if      (!strcmp(argv[a], "--host"))       host  = argv[++a];
        else if (!strcmp(argv[a], "--port"))       port  = (uint16_t)atoi(argv[++a]);
        else if (!strcmp(argv[a], "--sensors"))    count = atoi(argv[++a]);
        else if (!strcmp(argv[a], "--rate"))       rate  = atof(argv[++a]);
        else if (!strcmp(argv[a], "--fault-prob")) fprob = atof(argv[++a]);
    }
    if (count < 1 || count > 1024) { fprintf(stderr, "sensors must be 1-1024\n"); return 1; }
    if (rate <= 0.0)               { fprintf(stderr, "rate must be > 0\n");       return 1; }

    signal(SIGINT, handle_sigint);
    srand((unsigned)time(NULL) ^ (unsigned)getpid());

    sensor_t *sensors = calloc((size_t)count, sizeof *sensors);
    if (!sensors) { perror("calloc"); return 1; }
    for (int i = 0; i < count; i++) sensors[i].id = (uint16_t)(100 + i);

    fprintf(stderr, "gridpulse-sensor: %d sensors -> %s:%u at %.1f pkt/s each\n",
            count, host, port, rate);

    int fd = -1;
    uint8_t buf[PACKET_SIZE];
    struct timespec tick = {
        .tv_sec  = (time_t)(1.0 / rate),
        .tv_nsec = (long)(fmod(1.0 / rate, 1.0) * 1e9)
    };

    while (running) {
        if (fd < 0) {
            fd = connect_collector(host, port);
            if (fd < 0) {          /* collector down: retry with backoff */
                fprintf(stderr, "collector unreachable, retrying in 2s\n");
                sleep(2);
                continue;
            }
            fprintf(stderr, "connected to collector\n");
        }

        for (int i = 0; i < count && running; i++) {
            if (sensors[i].fault_ticks_left == 0 && frand() < fprob) {
                sensors[i].fault_ticks_left = 6 + (int)(frand() * 10);
                fprintf(stderr, "sensor %u: voltage sag begins\n", sensors[i].id);
            }
            build_packet(buf, &sensors[i]);
            if (send_all(fd, buf, PACKET_SIZE) < 0) {
                fprintf(stderr, "send failed (%s), reconnecting\n", strerror(errno));
                close(fd);
                fd = -1;
                break;
            }
        }
        nanosleep(&tick, NULL);
    }

    if (fd >= 0) close(fd);
    free(sensors);
    fprintf(stderr, "gridpulse-sensor: shutting down\n");
    return 0;
}
