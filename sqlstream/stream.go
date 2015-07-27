package sqlstream

import (
	"fmt"
	"io"

	"database/sql"
	_ "github.com/go-sql-driver/mysql" // Blank import for mysql driver
)

type SQLStream struct {
	db              *sql.DB
	sqlQuery        string
	recordDelimiter string
	lineDelimiter   string
}

type connectionInfo struct {
	username string
	password string
	dbName   string
	host     string
	port     int
}

func NewSQLStream(
	username string,
	password string,
	dbName string,
	host string,
	port int,
	sqlQuery string,
	recordDelimiter string,
	lineDelimiter string) (*SQLStream, error) {

	connectionInfo := newConnectionInfo(username, password, dbName, host, port)

	db, err := newDBConnection(connectionInfo)

	if err != nil {
		return nil, err
	}

	return &SQLStream{db: db,
		sqlQuery:        sqlQuery,
		recordDelimiter: recordDelimiter,
		lineDelimiter:   lineDelimiter}, nil
}

func (s *SQLStream) ExecuteQuery() (io.ReadCloser, error) {

	rows, err := s.db.Query(s.sqlQuery)

	if err != nil {
		return nil, err
	}

	reader := SQLReader(rows, s.recordDelimiter, s.lineDelimiter)

	return reader, nil
}

func newConnectionInfo(
	username string,
	password string,
	dbName string,
	host string,
	port int) connectionInfo {

	return connectionInfo{username: username,
		password: password,
		dbName:   dbName,
		host:     host,
		port:     port}
}

func newDBConnection(conn connectionInfo) (*sql.DB, error) {

	connFmt := "%s:%s@tcp(%s:%d)/%s"
	driver := "mysql"

	connectionString := fmt.Sprintf(connFmt,
		conn.username,
		conn.password,
		conn.host,
		conn.port,
		conn.dbName)

	db, err := sql.Open(driver, connectionString)

	return db, err
}
