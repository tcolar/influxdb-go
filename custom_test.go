// History: Feb 24 14 tcolar Creation

package influxdb

import (
	"testing"
)

func TestCustom(t *testing.T) {
	client, err := NewClient(&ClientConfig{
		Username: "dbuser",
		Password: "pass",
		Database: "foobar",
	})

	if err != nil {
		t.Error(err)
	}

	err = client.DeleteSeries("testcustom")
	if err != nil {
		t.Error(err)
	}

	series := &Series{
		Name:    "testcustom",
		Columns: []string{"value"},
		Points: [][]interface{}{
			[]interface{}{7.0},
		},
	}
	if err := client.WriteSeries([]*Series{series}); err != nil {
		t.Error(err)
	}

	result, err := client.QuerySingle("select * from testcustom")
	if err != nil {
		t.Error(err)
	}

	var target float64
	err = result.PtVal(&target, "value")
	if err != nil {
		t.Error(err)
	}
	if target != 7 {
		t.Errorf("Expected 7, got %f", target)
	}
}
