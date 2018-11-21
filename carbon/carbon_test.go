package carbon

import (
	"math/rand"
	"strconv"
	"testing"
)

func gimmeCarbon() Carbon {
	return Carbon{
		Host:     "localhost",
		Port:     2003,
		Protocol: "tcp",
	}
}

func TestCarbonConnect(t *testing.T) {
	c := gimmeCarbon()
	if err := c.Connect(); err != nil {
		t.Error(err)
	}
}

func TestSendMetrics(t *testing.T) {
	c := gimmeCarbon()
	c.Connect()
	var metrics []Metric
	// Never fails
	metrics = append(metrics, Metric{
		Name:  "stone.number",
		Value: strconv.Itoa(rand.Intn(100)),
	})
	if err := c.SendMetrics(metrics); err != nil {
		t.Error(err)
	}
}
