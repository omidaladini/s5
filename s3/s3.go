package s3

import (
	"bytes"
	"io"
	"log"
	"time"

	"github.com/goamz/goamz/aws"
	"github.com/goamz/goamz/s3"
)

type S3Creds struct {
	AccessKey string
	SecretKey string
}

type S3MultipartUploadSession struct {
	creds         S3Creds
	s3Region      string
	s3Bucket      string
	chunkSizeInMB int
}

func CleanupChunks(s3AccessKey string, s3SecretKey string, s3Region string, s3Bucket string) {
	auth := aws.Auth{
		AccessKey: s3AccessKey,
		SecretKey: s3SecretKey}

	b := s3.New(auth, aws.Regions[s3Region]).Bucket(s3Bucket)

	multis, _, _ := b.ListMulti("", "")

	for _, m := range multis {
		log.Println("Removing part files for: ", m)
		m.Abort()
	}
}

func NewS3Creds(accessKey string, secretKey string) S3Creds {
	return S3Creds{AccessKey: accessKey, SecretKey: secretKey}
}

func NewS3MultipartUpload(s3AccessKey string,
	s3SecretKey string,
	s3Region string,
	s3Bucket string,
	chunkSizeInMB int) S3MultipartUploadSession {
	return S3MultipartUploadSession{s3Bucket: s3Bucket,
		s3Region:      s3Region,
		chunkSizeInMB: chunkSizeInMB,
		creds: S3Creds{
			AccessKey: s3AccessKey,
			SecretKey: s3SecretKey}}
}

func (s *S3MultipartUploadSession) awsAuth() aws.Auth {
	return aws.Auth{
		AccessKey: s.creds.AccessKey,
		SecretKey: s.creds.SecretKey}
}

func (s *S3MultipartUploadSession) getS3Bucket() *s3.Bucket {
	auth := s.awsAuth()
	s3 := s3.New(auth, aws.Regions[s.s3Region])

	s3.ConnectTimeout = time.Second * 10
	s3.ReadTimeout = time.Second * 20
	s3.WriteTimeout = time.Second * 20
	s3.RequestTimeout = time.Second * 120

	return s3.Bucket(s.s3Bucket)
}

func (s *S3MultipartUploadSession) UploadMultiPart(reader io.Reader, s3Path string) error {

	bucket := s.getS3Bucket()

	multi, err := bucket.InitMulti(s3Path, "text/plain", s3.Private)

	if err != nil {
		return err
	}

	parts, err := s.uploadMultiPart(multi, reader)

	var errCleanup error = nil

	if err != nil {
		log.Printf("Multipart upload for %s failed, aborting: %v\n", s3Path, err)
		errCleanup = multi.Abort()
	} else {
		log.Printf("Multipart upload for %s successful, stitching the parts.\n", s3Path)
		errCleanup = multi.Complete(parts)
	}

	if errCleanup != nil {
		log.Println("Abort or complete for %s failed. You should cleanup the parts manually: %v. Original error %v\n", s3Path, errCleanup, err)
		return errCleanup
	}

	return err
}

func (s *S3MultipartUploadSession) uploadMultiPart(multi *s3.Multi, reader io.Reader) ([]s3.Part, error) {

	var parts []s3.Part
	var totalBytesUploaded = 0
	const retryCount = 3

	dataChan := make(chan []byte)
	readFailure := make(chan error)
	sendFailure := make(chan struct{})
	errors := make(chan error, 2)

	go func() {
		defer close(dataChan)
		defer close(readFailure)
		for {
			data, errRead := readSinglePart(reader, s.chunkSizeInMB)
			select {
			case dataChan <- data:
				if errRead != nil {
					if errRead != io.EOF {
						readFailure <- errRead
						errors <- errRead
					}
					return
				}
			case <-sendFailure:
				return
			}

		}
	}()

	go func() {
		defer close(errors)
		defer close(sendFailure)
		for data := range dataChan {

			part, errUpload := uploadSinglePart(multi, len(parts)+1, data, retryCount)

			totalBytesUploaded = totalBytesUploaded + len(data)
			parts = append(parts, *part)

			log.Printf("%d parts of %s and ~%dMB of data has been uploaded",
				len(parts),
				multi.Key,
				totalBytesUploaded/(1024.0*1024.0))

			if errUpload != nil {
				sendFailure <- struct{}{}
				errors <- errUpload
				return
			}

			select {
			case err, _ := <-readFailure:
				if err != nil {
					return
				}
			default:
			}
		}
	}()

	for err := range errors {
		return nil, err
	}

	return parts, nil
}

func readSinglePart(reader io.Reader, chunkSizeInMB int) ([]byte, error) {
	data := make([]byte, 1024*1024*chunkSizeInMB)

	readLen, errRead := reader.Read(data)
	data = data[:readLen]

	return data, errRead
}

func uploadSinglePart(multipartSession *s3.Multi, partNum int, data []byte, retry int) (*s3.Part, error) {

	part, err := multipartSession.PutPart(partNum, bytes.NewReader(data))

	if err != nil {
		if retry > 0 {
			log.Printf("Part %d for %s failed. Retrying. Error: %v", partNum, multipartSession.Key, err)
			return uploadSinglePart(multipartSession, partNum, data, retry-1)
		} else {
			log.Printf("Part %d for %s failed. Error: %v", partNum, multipartSession.Key, err)
			return nil, err
		}
	}

	return &part, err
}
