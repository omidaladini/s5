package gzreader

import "strings"
import "testing"
import "testing/quick"
import "compress/gzip"
import "bytes"
import "fmt"
import "errors"
import "io/ioutil"

const daisy = `Daisy, Daisy
				Give me your answer, do.
				I'm half crazy
				all for the love of you`

func checkDataDecompressesSuccessfully(str string) error {

	strReader := strings.NewReader(str)
	compressedReader := NewCompressedReader(strReader)
	compressedData, _ := ioutil.ReadAll(compressedReader)

	decompressingReader, _ := gzip.NewReader(bytes.NewReader(compressedData))
	decompressedData, _ := ioutil.ReadAll(decompressingReader)

	if string(decompressedData) != str {
		return errors.New("Data didn't compress successfully.")
	}

	if ratio, err := compressedReader.CompressionRatio(); str != "" &&
		(ratio <= 1 || err != nil) {
		return fmt.Errorf("Compression ratio abnormal %f", ratio)
	}

	return nil
}

func TestDataDecompressesSuccessfully(t *testing.T) {

	if checkDataDecompressesSuccessfully(daisy) != nil {
		t.Errorf("Testing for compression of daisy poem failed")
	}

	f := func(str string) bool {
		return nil == checkDataDecompressesSuccessfully(str)
	}

	if err := quick.Check(f, nil); err != nil {
		t.Errorf("Compression check failed: ", err)
	}
}

func TestCompressionRatioUnknown(t *testing.T) {

	strReader := strings.NewReader(daisy)
	compressedReader := NewCompressedReader(strReader)
	if _, err := compressedReader.CompressionRatio(); err == nil {
		t.Errorf("Compression ratio must be undefinded when no data read.")
	}
}
