package sqlstream

import (
	"errors"
	"fmt"
	"io"
	"strings"
)

// This is all we need from sql.Rows
type DataRows interface {
	Columns() ([]string, error)
	Close() error
	Next() bool
	Err() error
	Scan(dest ...interface{}) error
}

type sqlReader struct {
	rows       DataRows
	nullString string //TODO implement this in code

	recordDelimiter string
	lineDelimiter   string

	buffer []byte
	eof    bool
}

// SQLReader represents a live output of a Sql query
func SQLReader(rows DataRows, recordDelimiter string, lineDelimiter string) io.ReadCloser {
	return &sqlReader{
		rows:            rows,
		buffer:          nil,
		recordDelimiter: "\t",
		lineDelimiter:   "\n",
		eof:             false}
}

func (r *sqlReader) Read(p []byte) (n int, err error) {

	colNames, colReadError := r.rows.Columns()

	if colReadError != nil {
		return 0, errors.New("Failed to retrieve columns list")
	}

	readCols := make([]interface{}, len(colNames))
	writeCols := make([][]byte, len(colNames))

	for i := range writeCols {
		readCols[i] = &writeCols[i]
	}

	//Read rows until we have enough bytes as requested
	for len(r.buffer) < len(p) {
		if !r.rows.Next() {

			if r.rows.Err() != nil {
				return 0, r.rows.Err()
			}

			r.eof = true
			break
		}

		if scanError := r.rows.Scan(readCols...); scanError != nil {
			return 0, fmt.Errorf("Error scanning retrieved sql columns", scanError)
		}

		tidyCols := r.deserializeData(writeCols)

		acquired := []byte(strings.Join(tidyCols, r.recordDelimiter) + r.lineDelimiter)
		r.buffer = append(r.buffer, acquired...)
	}

	readLen := min(len(r.buffer), len(p))

	copy(p, r.buffer[:readLen])
	r.buffer = r.buffer[readLen:]

	if r.eof {
		return readLen, io.EOF
	}

	return readLen, nil
}

func (r *sqlReader) Close() error {

	if err := r.rows.Close(); err != nil {
		return err
	}

	return nil
}

func (r *sqlReader) deserializeData(rawColumns [][]byte) []string {

	tidyCols := make([]string, len(rawColumns))

	for i := range rawColumns {
		if rawColumns[i] == nil {
			tidyCols[i] = "NULL"
		} else {
			raw := string(rawColumns[i])
			//TODO Escape delimiters better. Possibly get these replacements from user
			delimEscaped := strings.Replace(raw, r.recordDelimiter, " ", -1)
			delimEscaped = strings.Replace(delimEscaped, r.lineDelimiter, " ", -1)
			tidyCols[i] = delimEscaped
		}
	}

	return tidyCols
}

// WTF Golang! You haz no min/max for int?
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
