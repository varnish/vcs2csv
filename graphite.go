package main

import (
	"fmt"
	"net/url"

	"github.com/varnish/go-pkgs/logger"
	"github.com/varnish/vcs2csv/carbon"
)

// VCS2Graphite implments VC2Anyer interface
type VCS2Graphite struct{}

func (c *VCS2Graphite) send(carb *carbon.Carbon, e *Entry) error {

	// Loop over entries we got
	for _, b := range e.Buckets {

		// Create a map of the data, make it much easier to send later on.
		out := b.ToMap()

		// Urlencode the key to make the key safer
		encodedKey := url.QueryEscape(e.Key)

		// We want to send the metrics in bulk so we create our
		// slice here to be used later.
		var metrics []carbon.Metric

		// We want to create carbon metrics like
		// <vcs>.<key>.<metric> <value>
		for k, v := range out {
			// No need for timestamp as a metric, so filter it out.
			if k == "timestamp" {
				continue
			}
			name := fmt.Sprintf("vcs.%s.%s.%s", *carbonPath, encodedKey, k)
			metrics = append(metrics,
				carbon.Metric{
					Name:  name,
					Value: v,
				})
		}

		err := carb.SendMetrics(metrics)
		if err != nil {
			return fmt.Errorf("error in sending metrics: %v", err)
		}
	}

	return nil

}

// HandleEntry takes a chan of Entry (with buckets) and a done
// channel for quit, writes output to Carbon
func (c *VCS2Graphite) HandleEntry(e chan *Entry, done chan bool) {

	// Create carbon stats endpoint
	carb := carbon.Carbon{
		Host:     *carbonHost,
		Port:     *carbonPort,
		Protocol: *carbonProto,
		Log:      true,
	}
	// Connect to carbon, if it fails we will try again when sending
	if err := carb.Connect(); err != nil {
		msg := fmt.Sprintf("Error connecting to carbon server, will retry: %s:%s:%d",
			*carbonProto, *carbonHost, *carbonPort,
		)
		logger.Error(msg)
	}
	defer carb.Close()

	for {
		select {
		case e, ok := <-e:
			if !ok {
				logger.Debug("channel closed, we are done here.")
				return
			}
			// Send metrics, log errors, if it fails, skip and move on.
			if err := c.send(&carb, e); err != nil {
				logger.Debug("Error sending data, skipping", err.Error())
			}

		case <-done:
			logger.Debug("done")
			return
		}

	}
}
