package s5

import (
	"log"
	"os"

	"github.com/omidaladini/s5/gzreader"
	"github.com/omidaladini/s5/s3"
	"github.com/omidaladini/s5/sqlstream"
)

type S5 struct {
	SqlHost     string
	SqlUser     string
	SqlPort     int
	SqlPassword string
	SqlDatabase string
	SqlQuery    string

	S3AccessKey string
	S3SecretKey string
	S3Region    string
	S3Bucket    string
	S3Path      string

	ChunkSizeInMB int
	Compress      bool

	RecordDelimiter string
	LineDelimiter   string
}

func (s *S5) Run() error {

	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags)

	//Prepare SQL connection
	sqlStream, err := sqlstream.NewSQLStream(
		s.SqlUser,
		s.SqlPassword,
		s.SqlDatabase,
		s.SqlHost,
		s.SqlPort,
		s.SqlQuery,
		s.RecordDelimiter,
		s.LineDelimiter)

	if err != nil {
		return err
	}

	//Execute SQL query
	sqlReader, err := sqlStream.ExecuteQuery()

	if err != nil {
		return err
	}

	if ! sqlReader.HasData() {
		return nil
	}

	defer sqlReader.Close()

	//Incrementally Upload to S3
	s3Stream := s3.NewS3MultipartUpload(
		s.S3AccessKey,
		s.S3SecretKey,
		s.S3Region,
		s.S3Bucket,
		s.ChunkSizeInMB)

	if s.Compress {
		compressedReader := gzreader.NewCompressedReader(sqlReader)
		err = s3Stream.UploadMultiPart(compressedReader, s.S3Path)
	} else {
		err = s3Stream.UploadMultiPart(sqlReader, s.S3Path)
	}

	if err != nil {
		return err
	}

	return nil
}
