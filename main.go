package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/varnish/go-pkgs/logger"
)

var (
	hostFlag    = flag.String("listen-host", "127.0.0.1", "Listen host")
	portFlag    = flag.Int("listen-port", 6556, "Listen port")
	keysFlag    = flag.String("keys", "ALL", "VCS keys to include")
	dirFlag     = flag.String("directory", "/var/lib/vcs2csv/", "Directory to store CSV files")
	logLevel    = flag.String("log", "INFO", "Log level [DEBUG, INFO, WARNING, ERROR, QUIET]")
	carbonHost  = flag.String("carbon-host", "", "Carbon Host")
	carbonPort  = flag.Int("carbon-port", 2003, "Carbon Port")
	carbonProto = flag.String("carbon-proto", "tcp", "Carbon protocl [udp|tcp]")
	carbonPath  = flag.String("carbon-path", "vcs2csv", "Carbon path built vcs.<carbon-path>.bucket.metric")
)

// VCS2Anyer is the interface that all outputers need to implement
type VCS2Anyer interface {
	HandleEntry(chan *Entry, chan bool)
}

type Bucket struct {
	Timestamp     string `json:"timestamp"`
	N_requests    string `json:"n_requests"`
	N_req_uniq    string `json:"n_req_uniq"`
	N_misses      string `json:"n_misses"`
	N_restarts    string `json:"n_restarts"`
	Ttfb_miss     string `json:"ttfb_miss"`
	Ttfb_hit      string `json:"ttfb_hit"`
	N_bodybytes   string `json:"n_bodybytes"`
	Respbytes     string `json:"respbytes"`
	Reqbytes      string `json:"reqbytes"`
	Bereqbytes    string `json:"bereqbytes"`
	Berespbytes   string `json:"berespbytes"`
	Resp_code_1xx string `json:"resp_code_1xx"`
	Resp_code_2xx string `json:"resp_code_2xx"`
	Resp_code_3xx string `json:"resp_code_3xx"`
	Resp_code_4xx string `json:"resp_code_4xx"`
	Resp_code_5xx string `json:"resp_code_5xx"`
}

type Entry struct {
	Key     string   `json:"key"`
	Buckets []Bucket `json:"buckets"`
}

// ToSlice returns bucket keys as a slice of strings
func (b Bucket) ToSlice() []string {
	var s []string
	s = append(s, strings.TrimSpace(b.Timestamp))
	s = append(s, strings.TrimSpace(b.N_requests))
	s = append(s, strings.TrimSpace(b.N_req_uniq))
	s = append(s, strings.TrimSpace(b.N_misses))
	s = append(s, strings.TrimSpace(b.N_restarts))
	s = append(s, strings.TrimSpace(b.Ttfb_miss))
	s = append(s, strings.TrimSpace(b.Ttfb_hit))
	s = append(s, strings.TrimSpace(b.N_bodybytes))
	s = append(s, strings.TrimSpace(b.Respbytes))
	s = append(s, strings.TrimSpace(b.Reqbytes))
	s = append(s, strings.TrimSpace(b.Bereqbytes))
	s = append(s, strings.TrimSpace(b.Berespbytes))
	s = append(s, strings.TrimSpace(b.Resp_code_1xx))
	s = append(s, strings.TrimSpace(b.Resp_code_2xx))
	s = append(s, strings.TrimSpace(b.Resp_code_3xx))
	s = append(s, strings.TrimSpace(b.Resp_code_4xx))
	s = append(s, strings.TrimSpace(b.Resp_code_5xx))
	return s
}

// ToMap return bucket names as keys with values
func (b Bucket) ToMap() map[string]string {
	m := make(map[string]string)
	m["timestamp"] = b.Timestamp
	m["n_requests"] = b.N_requests
	m["n_req_uniq"] = b.N_req_uniq
	m["n_misses"] = b.N_misses
	m["n_restarts"] = b.N_restarts
	m["ttfb_miss"] = b.Ttfb_miss
	m["ttfb_hit"] = b.Ttfb_hit
	m["n_bodybytes"] = b.N_bodybytes
	m["respbytes"] = b.Respbytes
	m["reqbytes"] = b.Reqbytes
	m["bereqbytes"] = b.Bereqbytes
	m["berespbytes"] = b.Berespbytes
	m["resp_code_1xx"] = b.Resp_code_1xx
	m["resp_code_2xx"] = b.Resp_code_2xx
	m["resp_code_3xx"] = b.Resp_code_3xx
	m["resp_code_4xx"] = b.Resp_code_4xx
	m["resp_code_5xx"] = b.Resp_code_5xx
	return m
}

// vcsConHandler handles incoming connections and fires of an
// VCS2Anyer to handle the incoming data.
func vcsConHandler(conn net.Conn, vany VCS2Anyer) {
	// Channel to read/write Entry to
	entryChan := make(chan *Entry)
	doneChan := make(chan bool)
	defer close(entryChan)
	defer close(doneChan)
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	split := func(data []byte, atEOF bool) (int, []byte, error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}
		if i := bytes.Index(data, []byte{'\n', '\n'}); i >= 0 {
			// We have a full event
			return i + 2, data[0:i], nil
		}
		// If we're at EOF, we have a final event
		if atEOF {
			return len(data), data, nil
		}
		// Request more data.
		return 0, nil, nil
	}
	scanner.Split(split)

	// Start our Vcs2anyer to handle the Entry
	go vany.HandleEntry(entryChan, doneChan)

	// Set the split function for the scanning operation.
	for scanner.Scan() {
		entry := scanner.Bytes()
		//log.Println("New event")

		// Remove the first line of the entry, that
		// contains the number of bytes to read.
		e := Entry{}
		idx := bytes.IndexByte(entry, '\n')
		// If we fail to indexbyte we will panic, so check for -1
		// if there is no \n and continue.
		if idx == -1 {
			logger.Info("invalid data recieved")
			continue
		}
		// If idx is -1 this will panic
		entry = entry[idx:]

		// Unmarshal JSON from VCS into the Entry struct
		if err := json.Unmarshal(entry, &e); err != nil {
			logger.Info("Ignoring unparseable input data.")
			continue
		}

		// Skip keys that we are not looking for
		exists := contains(strings.Split(*keysFlag, " "), e.Key)
		if !exists {
			continue
		}
		// Send the entry to the handler
		entryChan <- &e
	}
	// We are done, make sure handlers quit
	logger.Debug("Connection ended")
	doneChan <- true
}

// Check if a string exists in slice of strings
func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

// getLogLevel returns integer representation of log level
func getLogLevel(level string) int {
	var selectedLevel = logger.INFO_LEVEL

	if strings.EqualFold("debug", level) {
		selectedLevel = logger.DEBUG_LEVEL
	} else if strings.EqualFold("warning", level) {
		selectedLevel = logger.WARNING_LEVEL
	} else if strings.EqualFold("error", level) {
		selectedLevel = logger.ERROR_LEVEL
	} else if strings.EqualFold("quiet", level) {
		selectedLevel = logger.DISABLED
	}

	return selectedLevel
}

func main() {
	flag.Parse()
	var daemonLogLevel = getLogLevel(*logLevel)
	logger.InitNewLogger(os.Stdout, daemonLogLevel)

	l, err := net.Listen("tcp", *hostFlag+":"+strconv.Itoa(*portFlag))
	if err != nil {
		logger.ErrorSync(err.Error())
		os.Exit(-1)
	}
	defer l.Close()

	logger.Info(fmt.Sprintf("Startinv vcs2any, listening on %s:%d for incomming vcs data", *hostFlag, *portFlag))

	var anyer VCS2Anyer

	// Choose VCSAnyer based on flags.
	if *carbonHost != "" {
		logger.Info("Using Carbon/Graphite for storing metrics")
		anyer = &VCS2Graphite{}
	} else {
		logger.Info("Using CSV for storing metrics")
		anyer = &VCS2csv{}
	}

	for {
		// Wait for a connection.
		conn, err := l.Accept()
		if err != nil {
			logger.ErrorSync(err.Error())
			os.Exit(-1)
		}

		// Handle the connection in a new goroutine.
		// The loop then returns to accepting, so that
		// multiple connections may be served concurrently.
		logger.Info(fmt.Sprintf("New connection from: %s", conn.RemoteAddr()))

		// Fire off our handler for this connection, will use
		// VCSAnyer to send the metrics.
		go vcsConHandler(conn, anyer)
	}
}
