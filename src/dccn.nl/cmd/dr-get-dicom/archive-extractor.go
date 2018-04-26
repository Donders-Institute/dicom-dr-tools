package main

import (
	"io"
	"os"
	"fmt"
	"errors"
	"strings"
	"archive/tar"
	"compress/gzip"
	"path/filepath"
	log "github.com/sirupsen/logrus"
)

// Extractor is an interface defining the methods for extracting an archive file.
type Extractor interface {
	Extract(pathArchive string) (string, error)
}

// GetDicomExtractor returns the DicomExtractor implementation based on the
// given path.  The selection is made based on the suffix of the path.
func GetDicomExtractor(path string) (Extractor) {
	if strings.HasSuffix(path, ".tar.gz") {
		return &DicomExtractorTgz{}
	}
	if strings.HasSuffix(path, ".zip") {
		return &DicomExtractorZip{}
	}
	return &DicomExtractorIgnore{}
}

// DicomExtractorTgz implements the Extractor interface for the gzipped-tar archive.
// It extract the first DICOM file from the archive, and store the extracted file
// in a directory the same as the gzipped-tar file.
type DicomExtractorTgz struct {}
func (DicomExtractorTgz) Extract(pathArchive string) (string, error) {

	// open the gizpped archive: file -> gzip -> tar
	f, err := os.Open(pathArchive)
	if err != nil {
		return "", err
	}
	a, err := gzip.NewReader(f)
	if err != nil {
		return "", err
	}
	tr := tar.NewReader(a)

	defer func() {
		a.Close()
		f.Close()
		os.Remove(pathArchive)
	}()

	dir := filepath.Dir(pathArchive)

	// extracted dicom filename
	var fdicom string
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return "", errors.New("empty archive: " + pathArchive)
		}
		if h.Typeflag == tar.TypeDir {
			continue
		}
		if h.Typeflag == tar.TypeReg {
			// output file
			fdicom = filepath.Join(dir, filepath.Base(h.Name))

			if err := copyReaderToPath(tr, fdicom, os.FileMode(h.Mode)); err != nil {
				return "", err
			}

			log.Debug(fmt.Sprintf("DICOM file extracted: %s", fdicom))
			return fdicom, nil
		}
	}
}

// DicomExtractorIgnore implements the Extractor interface for ignoring extracting files from the archive.
type DicomExtractorIgnore struct {}
func (DicomExtractorIgnore) Extract(pathArchive string) (string, error) {
	return pathArchive, nil
}

// DicomExtractorIgnore implements the Extractor interface for extracting DICOM files from a zip file.
type DicomExtractorZip struct {}
func (DicomExtractorZip) Extract(pathArchive string) (string, error) {
	return pathArchive, nil
}
