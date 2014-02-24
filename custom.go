// History: Feb 22 14 tcolar Creation

package influxdb

import (
	//"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// QuerySingle querys and expect a single series as a result
func (c *Client) QuerySingle(query string) (series *Series, err error) {

	result, err := c.Query(query)
	if err != nil {
		return series, err
	}
	if len(result) != 1 {
		return series, errors.New(fmt.Sprintf("Expected one result, got %d", len(result)))
	}
	return result[0], err
}

// DeleteSeries deletes a series
func (c *Client) DeleteSeries(series string) error {
	url := c.getUrl(fmt.Sprintf("/db/%s/series/%s", c.database, series))
	resp, err := c.del(url)
	return responseToError(resp, err, true)
}

// PtVal gets a single value(identified by colName) from a single point into target
func (s *Series) PtVal(target interface{}, colName string) (err error) {
	val := reflect.ValueOf(target)
	if val.Kind() != reflect.Ptr {
		return fmt.Errorf("Cannot map the point to non-pointer (%s)", val.Type().String())
	}
	index := s.ColIndex(colName)
	if index == -1 {
		return fmt.Errorf("The column %s was not found in the series %s", colName, s.Columns)
	}
	if len(s.Points) != 1 {
		return fmt.Errorf("Expected a single point, got %d", len(s.Points))
	}
	reflect.Indirect(val).Set(reflect.ValueOf(s.Points[0][index]))
	return err
}

// ColIndex finds the Index of a column in the series
// -1 if not found
func (s *Series) ColIndex(colName string) int {
	col := strings.ToLower(colName)
	for i, c := range s.Columns {
		if strings.ToLower(c) == col {
			return i
		}
	}
	return -1
}
