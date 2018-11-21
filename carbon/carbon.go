package carbon

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"time"
)

const defaultTimeout = 5

type Carbon struct {
	Host        string
	Port        int
	Protocol    string
	Timeout     time.Duration
	conn        net.Conn
	Log         bool
	isConnected bool
	Prefix      string
}

func (c *Carbon) debug(format string, v ...interface{}) {
	if c.Log {
		format = "Carbon - " + format
		log.Printf(format, v...)
	}
}

// Connect connects to carbon server
func (c *Carbon) Connect() error {
	c.debug("Connecting to: %s:%s:%d", c.Protocol, c.Host, c.Port)
	address := fmt.Sprintf("%s:%d", c.Host, c.Port)

	// If no timeout, set a default timeout
	if c.Timeout == 0 {
		c.debug("Setting default timeout: %d", defaultTimeout)
		c.Timeout = defaultTimeout * time.Second
	}

	var err error
	var conn net.Conn

	if c.Protocol == "udp" {
		udpAddr, err := net.ResolveUDPAddr("udp", address)
		if err != nil {
			return err
		}
		conn, err = net.DialUDP(c.Protocol, nil, udpAddr)
	} else {
		conn, err = net.DialTimeout(c.Protocol, address, c.Timeout)
	}

	c.debug("Connected")

	if err != nil {
		return err
	}
	c.conn = conn
	c.isConnected = true
	return nil
}

// Close end the connection to carbon server
func (c *Carbon) Close() error {
	c.debug("Closing connection")
	err := c.conn.Close()
	c.conn = nil
	return err
}

// SendMetrics takes a slice of type Metric and batch-sends
func (c *Carbon) SendMetrics(metrics []Metric) error {

	// We reconnect if we are not connected, this should
	// maybe be handled in the caller instead?
	if c.isConnected == false {
		if err := c.Connect(); err != nil {
			log.Printf("Failed to reconnect, will retry: %s:%s:%d",
				c.Protocol, c.Host, c.Port)
		}
	}

	buf := bytes.NewBufferString("")

	for _, metric := range metrics {
		if metric.Name == "" || metric.Value == "" {
			continue
		}
		if metric.Timestamp == 0 {
			metric.Timestamp = time.Now().Unix()
		}
		if c.Protocol == "udp" {
			fmt.Fprintf(c.conn, "%s %s %d\n", metric.Name, metric.Value, metric.Timestamp)
			continue
		}
		buf.WriteString(fmt.Sprintf("%s %s %d\n", metric.Name, metric.Value, metric.Timestamp))
	}

	if c.Protocol == "tcp" {
		//fmt.Print("Sending msg:", buf.String(), "'")
		if _, err := c.conn.Write(buf.Bytes()); err != nil {
			return err
		}
	}
	c.debug("Sent %d metrics", len(metrics))
	return nil
}

// Metric holds a simple Carbon metric
type Metric struct {
	Name      string
	Value     string
	Timestamp int64
}

// NewMetric returns a new metric
func NewMetric(name, value string, timestamp int64) Metric {
	return Metric{
		Name:      name,
		Value:     value,
		Timestamp: timestamp,
	}
}

// String returns a string representation of the metric
func (metric Metric) String() string {
	return fmt.Sprintf(
		"%s %s %s",
		metric.Name,
		metric.Value,
		time.Unix(metric.Timestamp, 0).Format("2006-01-02 15:04:05"),
	)
}
