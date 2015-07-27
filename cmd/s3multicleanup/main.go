package main

import (
	"log"
	"os"

	"github.com/omidaladini/s5/s3"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	cl = kingpin.New("s3multicleanup", "Cleans up left-over chunks from unfinished S3 multipart uploads.")

	s3AccessKey = cl.Flag("s3.accesskey", "S3 Access Key").Required().String()
	s3SecretKey = cl.Flag("s3.secretkey", "S3 Secret Key").Required().String()
	s3Region    = cl.Flag("s3.region", "S3 Region").Required().String()
	s3Bucket    = cl.Flag("s3.bucket", "S3 Bucket").Required().String()
)

func main() {
	kingpin.MustParse(cl.Parse(os.Args[1:]))

	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags)

	s3.CleanupChunks(*s3AccessKey, *s3SecretKey, *s3Region, *s3Bucket)
}
