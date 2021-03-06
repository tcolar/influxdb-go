// History: Feb 22 14 tcolar Creation

package influxdb

import (
	"errors"
	"fmt"
	//"log"
	"reflect"
	"strings"
)

// QuerySingle runs a query and retuens and expects a single series as a result
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

// SeriesMerge creates a merged serie from the provided series (merge by "time")
// All series should be based on the same time interval for this to be useful
// ColMapping is used to specify which columns we want returned and what to name them.
// Each ColMapping is applied to the matching series (by index)
func SeriesMerge(name string, series []*Series, colMapping []map[string]string) *Series {
	allCols := []string{"time"}
	merged := Series{
		Name: name,
	}
	pts := map[float64][]interface{}{}
	for i, s := range series {
		indexes := []int{}
		for k, v := range colMapping[i] {
			index := s.ColIndex(k)
			if index != -1 {
				allCols = append(allCols, v)
				indexes = append(indexes, index)
			}
		}
		var t float64 = 0.12345
		for _, point := range s.Points {
			// temp bug workaround for https://github.com/influxdb/influxdb/issues/321#issuecomment-37207763
			v := point[0].(float64)
			if t == v {
				continue
			}
			t = v
			// end bug
			pt := []interface{}{}
			for _, index := range indexes {
				pt = append(pt, point[index])
			}
			time := point[0].(float64)
			for len(pts[time]) < i { // padding missing values with 0
				pts[time] = append(pts[time], 0.0)
			}
			// "Merge" columns of points with same timestamp together
			pts[time] = append(pts[time], pt...)
		}
	}
	merged.Columns = allCols
	for time, vals := range pts {
		pt := []interface{}{time}
		pt = append(pt, vals...)
		for len(pt) < len(allCols) { // padding missing values with 0
			pt = append(pt, 0.0)
		}
		merged.Points = append(merged.Points, pt)
	}
	return &merged
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
