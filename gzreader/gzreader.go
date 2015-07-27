package gzreader

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"log"
)

// Reader to reader, funk to funky
func NewCompressedReader(src io.Reader) *CompressedReader {

	var b bytes.Buffer
	gzipWriter, _ := gzip.NewWriterLevel(&b, gzip.BestSpeed)

	return &CompressedReader{src: src,
		gzipWriter:   gzipWriter,
		buf:          &b,
		readBytes:    0,
		writtenBytes: 0}
}

type CompressedReader struct {
	src        io.Reader
	gzipWriter *gzip.Writer
	buf        *bytes.Buffer

	readBytes    int64
	writtenBytes int64
}

func (r *CompressedReader) CompressionRatio() (float64, error) {

	if r.writtenBytes == 0 {
		return 0, errors.New("Compression ratio unknown.")
	}

	return 100 * float64(r.readBytes) / float64(r.writtenBytes), nil
}

func (r *CompressedReader) Read(p []byte) (n int, err error) {

	uncompressed := make([]byte, len(p))
	readLenUncompressed, err := r.src.Read(uncompressed)

	r.readBytes = r.readBytes + int64(readLenUncompressed)

	r.gzipWriter.Write(uncompressed[:readLenUncompressed])

	if err == io.EOF {
		r.gzipWriter.Close()
	}

	readLenCompressed, _ := r.buf.Read(p)

	r.writtenBytes = r.writtenBytes + int64(readLenCompressed)

	if ratio, err := r.CompressionRatio(); err != nil {
		log.Println("Compression ratio: %", ratio)
	}

	return readLenCompressed, err
}
