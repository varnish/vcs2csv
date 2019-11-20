package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"regexp"
)

var (
	hostFlag = flag.String("listen-host", "127.0.0.1", "Listen host")
	portFlag = flag.Int("listen-port", 6556, "Listen port")
	keysFlag = flag.String("keys", "ALL", "VCS keys to include, separated by whitespace")
	keyPatternsFlag = flag.String("key-patterns", "", "Regex pattern of VCS keys to include, separated by whitespace")
	dirFlag  = flag.String("directory", "/var/lib/vcs2csv/", "Directory to store CSV files")
)

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

func handler(conn net.Conn) {
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

	for scanner.Scan() {
		// Set the split function for the scanning operation.
		entry := scanner.Bytes()
		//log.Println("New event")

		// Remove the first line of the entry, that
		// contains the number of bytes to read.
		e := Entry{}
		entry = entry[bytes.IndexByte(entry, '\n'):]

		// Unmarshal JSON from VCS into the Entry struct
		if err := json.Unmarshal(entry, &e); err != nil {
			log.Printf("Ignoring unparseable input data.")
			continue
		}

		// Key matching
		if contains(strings.Split(*keysFlag, " "), e.Key) {
			// String comparison
			log.Println("Matched key " + e.Key + " (strcmp)");
		} else if patternMatch(strings.Split(*keyPatternsFlag, " "), e.Key) {
			// Regular expression matching
			log.Println("Matched key " + e.Key + " (regexp)");
		} else {
			// Skip this key if it's not a match
			continue
		}

		// We may receive multiple buckets for the same key at
		// the same time. For example if we've had connection
		// problems and VCS buffered the data in between.
		for _, b := range e.Buckets {
			// Create a slice that will later be written
			// to file for this bucket
			out := b.ToSlice()

			// Use the timestamp for the filename
			secs, err := strconv.ParseInt(b.Timestamp, 10, 64)
			if err != nil {
				log.Println(err)
				continue
			}
			ts := time.Unix(secs, 0)

			// Urlencode the key to make the filename safe
			encodedKey := url.QueryEscape(e.Key)

			// Generate path to file
			filename := fmt.Sprintf("%s-%s.csv.gz", ts.Format("2006-01-02"), encodedKey)
			path := filepath.Join(*dirFlag, filename)

			// Create the file if it does not exist and
			// append if it exists. This is done per bucket
			// in order to ensure that wewrite to the
			// correct file when flipping between dates.
			fp, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
			if err != nil {
				log.Fatal(err)
			}

			gp := gzip.NewWriter(fp)
			writer := csv.NewWriter(gp)
			if err := writer.Write(out); err != nil {
				log.Fatal(err)
			}

			// Flush and close for every bucket to allow
			// other processes to read updated data in
			// between writes.
			writer.Flush()
			gp.Close()
			fp.Close()
		}
	}
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

func patternMatch(s []string, e string) bool {
	var matched bool
	var err error
	for _, pattern := range s {
		matched, err = regexp.MatchString(pattern, e)
		if err != nil {
			log.Fatal("Regular expression problem: " + err.Error())
		}
		if matched {
			return true
		}
		matched = false
	}
	return false
}

func main() {
	flag.Parse()

	l, err := net.Listen("tcp", *hostFlag+":"+strconv.Itoa(*portFlag))
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	for {
		// Wait for a connection.
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		// Handle the connection in a new goroutine.
		// The loop then returns to accepting, so that
		// multiple connections may be served concurrently.
		go handler(conn)
	}
}
