package sqlstream

import (
	"io/ioutil"
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

const numTestColumns int = 4

type TestDataRows struct {
	records       [][numTestColumns]string
	currentRecord int
}

func (r *TestDataRows) getRecordsForComparison() [][numTestColumns]string {
	return r.records
}

func TestSQLReader(t *testing.T) {

	const recordDelimiter = "\t"
	const lineDelimiter = "\n"

	var numRecords int = rand.Intn(1000)
	rows := NewTestDataRows(numRecords)
	reader := NewSQLReader(rows, recordDelimiter, lineDelimiter)

	if !reader.HasData() {
		t.Errorf("Reader should have data")
	}

	readAll, _ := ioutil.ReadAll(reader)
	str := string(readAll)

	if strings.Count(str, recordDelimiter) != numRecords*(numTestColumns-1) {
		t.Errorf("Expected number of delimiters not existing %d", strings.Count(str, recordDelimiter))
	}

	if strings.Count(str, lineDelimiter) != numRecords {
		t.Errorf("Expected number of line breaks not existing %d", strings.Count(str, lineDelimiter))
	}

	records := rows.getRecordsForComparison()

	for i := range records {
		for j := range records[i] {
			if !strings.Contains(str, records[i][j]) {
				t.Errorf("Stream didn't contain all expected columns")
			}
		}
	}

}

func TestHasDataOnEmpty(t *testing.T) {

	const recordDelimiter = "\t"
	const lineDelimiter = "\n"

	rows := NewTestDataRows(0)
	reader := NewSQLReader(rows, recordDelimiter, lineDelimiter)

	if reader.HasData() {
		t.Errorf("Reader shouldn't have data with no rows")
	}

	// second invocation to test if state is saved correctly
	if reader.HasData() {
		t.Errorf("Reader shouldn't have data with no rows")
	}
}

func TestHasData(t *testing.T) {
	const recordDelimiter = "\t"
	const lineDelimiter = "\n"

	rows := NewTestDataRows(1)

	reader := NewSQLReader(rows, recordDelimiter, lineDelimiter)

	if !reader.HasData() {
		t.Errorf("Reader should have data")
	}

	// second invocation to test if state is saved correctly
	if !reader.HasData() {
		t.Errorf("Reader should have data")
	}
}

func NewTestDataRows(numRecords int) *TestDataRows {

	var records [][numTestColumns]string
	for i := 0; i < numRecords; i++ {
		records = append(records, [numTestColumns]string{
			randSeq(rand.Intn(10) + 1),
			randSeq(rand.Intn(10) + 1),
			randSeq(rand.Intn(10) + 1),
			randSeq(rand.Intn(10) + 1)})
	}
	return &TestDataRows{records: records, currentRecord: -1}
}

func (*TestDataRows) Columns() ([]string, error) {
	return []string{"id", "name", "address", "height"}, nil
}

func (t *TestDataRows) Close() error {
	return nil
}

func (t *TestDataRows) Err() error {
	return nil
}

func (t *TestDataRows) Next() bool {

	t.currentRecord++

	if t.currentRecord >= len(t.records) {
		return false
	}
	return true
}

func (t *TestDataRows) Scan(dest ...interface{}) error {

	//Disappointingly, the golang's sql driver itself is full of reflections
	//Look up here: https://golang.org/src/database/sql/convert.go
	for i := range dest {
		dpv := reflect.ValueOf(dest[i])
		dv := reflect.Indirect(dpv)
		dv.Set(reflect.ValueOf([]byte(t.records[t.currentRecord][i])))
	}
	return nil
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
