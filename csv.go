package main

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// VCS2csv implements VCS2Anyer interface
type VCS2csv struct{}

// HandleEntry takes a chan of Entry (with buckets) and a done chan (for quitting)
// writes to a CSV file.
func (c *VCS2csv) HandleEntry(e chan *Entry, done chan bool) {
	for {
		select {
		case e, ok := <-e:
			if !ok {
				// Channel closed and we are done here.
				return
			}
			// Write to csv file, log errors
			if err := c.writeCsv(e); err != nil {
				log.Println(err)
			}

		case <-done:
			return
		}
	}
}

// writeCsv writes the buckets to a csv file.
func (c *VCS2csv) writeCsv(e *Entry) error {
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
		// in order to ensure that we write to the
		// correct file when flipping between dates.
		fp, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			return fmt.Errorf("error opening csv metrics file: %v", err)
		}

		gp := gzip.NewWriter(fp)
		writer := csv.NewWriter(gp)
		if err := writer.Write(out); err != nil {
			return fmt.Errorf("error writing metrics to csv file: %v", err)
		}

		// Flush and close for every bucket to allow
		// other processes to read updated data in
		// between writes.
		writer.Flush()
		gp.Close()
		fp.Close()
	}

	return nil
}
