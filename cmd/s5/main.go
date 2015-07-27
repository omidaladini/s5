package main

import (
	"fmt"
	"gopkg.in/alecthomas/kingpin.v2"
	"log"
	"os"

	"github.com/omidaladini/s5/gzreader"
	"github.com/omidaladini/s5/s3"
	"github.com/omidaladini/s5/sqlstream"
)

var (
	cl = kingpin.New("s5", "An app to stream result of a SQL query to Amazon S3")

	sqlHost     = cl.Flag("sql.host", "DB host").Default("127.0.0.1").String()
	sqlUser     = cl.Flag("sql.user", "DB user name").Required().String()
	sqlPort     = cl.Flag("sql.port", "DB port").Default("3306").Int()
	sqlPassword = cl.Flag("sql.password", "DB password").Required().String()
	sqlDatabase = cl.Flag("sql.database", "Database the query will run against").Required().String()
	sqlQuery    = cl.Flag("sql.query", "Query to run on DB").Required().String()

	s3AccessKey   = cl.Flag("s3.accesskey", "S3 Access Key").Required().String()
	s3SecretKey   = cl.Flag("s3.secretkey", "S3 Secret Key").Required().String()
	s3Region      = cl.Flag("s3.region", "S3 Region").Required().String()
	s3Bucket      = cl.Flag("s3.bucket", "S3 Bucket").Required().String()
	s3Path        = cl.Flag("s3.path", "Destination path").Required().String()
	chunkSizeInMB = cl.Flag("chunksizemb", "Uncompressed chunk size to be read incrementally").Default("50").Int()
	compress      = cl.Flag("compress", "Enable gzip compression").Default("false").Bool()

	recordDelimiter = cl.Flag("rdelimiter", "record delimiter").Default("\t").String()
	lineDelimiter   = cl.Flag("ldelimiter", "line delimiter").Default("\n").String()
)

func main() {
	kingpin.MustParse(cl.Parse(os.Args[1:]))

	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags)
	log.SetPrefix(fmt.Sprintf("[sql://%s -> s3://%s/%s] ", *sqlDatabase, *s3Bucket, *s3Path))

	//Prepare SQL connection
	sqlStream, err := sqlstream.NewSQLStream(
		*sqlUser,
		*sqlPassword,
		*sqlDatabase,
		*sqlHost,
		*sqlPort,
		*sqlQuery,
		*recordDelimiter,
		*lineDelimiter)

	if err != nil {
		log.Fatal(err)
	}

	//Execute SQL query
	sqlReader, err := sqlStream.ExecuteQuery()

	if err != nil {
		log.Fatal(err)
	}

	defer sqlReader.Close()

	//Incrementally Upload to S3
	s3Stream := s3.NewS3MultipartUpload(
		*s3AccessKey,
		*s3SecretKey,
		*s3Region,
		*s3Bucket,
		*chunkSizeInMB)

	if *compress {
		compressedReader := gzreader.NewCompressedReader(sqlReader)
		err = s3Stream.UploadMultiPart(compressedReader, *s3Path)
	} else {
		err = s3Stream.UploadMultiPart(sqlReader, *s3Path)
	}

	if err != nil {
		log.Fatal(err)
	}

}
