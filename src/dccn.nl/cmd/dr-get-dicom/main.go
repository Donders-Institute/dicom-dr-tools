package main

import (
	"io"
	"os"
	"fmt"
	"sync"
	"flag"
	"time"
	"regexp"
	"errors"
	"strings"
	"archive/tar"
	"compress/gzip"
	"path/filepath"
	"github.com/go-cmd/cmd"
	log "github.com/sirupsen/logrus"
)

const (
        // DR_NS_DCCN is the DCCN's collection namespace in the Donders Repository
	DR_NS_DCCN  = "/rdm/di/dccn"
        // MAX_DOWNLOAD_W defines max. number of concurrent downloading threads 
	MAX_DOWNLOAD_W  = 4
)

// DR_NS_COLLS defines the catchall collections in the Donders Repository
var DR_NS_COLLS = []string {"DAC_3055010.01_490","DAC_3010000.01_173"}

var opts_date *string
var opts_ddir *string
var opts_verbose *bool

func init() {
	y, m, d := time.Now().Date()
	today := fmt.Sprintf("%d%02d%02d", y, m, d)
	opts_date = flag.String("t", today, "specify the date string in format YYYYmmdd")
	opts_ddir = flag.String("d", "/project/3055010.01", "specify the local `path` for storing the downloaded raw data")
	opts_verbose = flag.Bool("v", false, "set to print debug messages")
	flag.Usage = usage
	flag.Parse()

	// set logging
	log.SetOutput(os.Stderr)

	// set logging level
	llevel := log.InfoLevel
	if *opts_verbose {
    		llevel = log.DebugLevel
	}
	log.SetLevel(llevel)
}

func usage() {
	fmt.Printf("\nUsage: %s [OPTIONS]\n", os.Args[0])
	fmt.Printf("\nOPTIONS:\n")
	flag.PrintDefaults()
}

func main() {
	for _,coll := range DR_NS_COLLS {
		log.Debug(fmt.Sprintf("checking %s ...", coll))
		ns_coll := filepath.Join(DR_NS_DCCN, coll, "raw")
		chanDicoms := getOneDicom(ns_coll)
		for f := range chanDicoms {
			log.Info(f)
		}
	}
}

// getOneDicom gets DICOM files from the given collection namespace 'ns_coll'. It loops all
// subject/session sub-namespaces to find DICOM data collected on the specified date that is
// provided as the command-line argument.  After that, it downloads the DICOM file/archive to
// a local directory.  It returns a channel to which the paths of downloaded/extracted DICOM
// files are pushed.
//
// This function uses go routines for performing query and downloading actions concurrently.  The
// level of concurrency is confined by the constant MAX_DOWNLOAD_W.
func getOneDicom(ns_coll string) (chan string) {

	// define the query
	query := "SELECT COLL_NAME WHERE COLL_NAME LIKE '" + ns_coll + "/%" + *opts_date + "%'"

	// Disable output buffering, enable streaming
	o := cmd.Options{
		Buffered:  false,
		Streaming: true,
	}
	chanColls := make(chan string, 2*MAX_DOWNLOAD_W)
	collMap := make(map[string]bool)
	mutex := &sync.Mutex{}
	iquestCmdC := cmd.NewCmdOptions(o, "iquest", "--no-page", "%s", query)

	s := iquestCmdC.Start()
	m := 0
	go func() {
		Loop:
		for {
			select {
			case line := <-iquestCmdC.Stdout:
				if m, _ := regexp.MatchString("[0-9]{3}-.*", line); m {
					// the collection contains series id. In this case, we
					// check if the same subject/session has been visited, and
					// only account the collection hasn't been visited before.
					if ! collMap[filepath.Dir(line)] {
						mutex.Lock()
						collMap[filepath.Dir(line)] = true
						mutex.Unlock()
						chanColls <- line
					}
					continue
				}
				chanColls <- line
			case line := <-iquestCmdC.Stderr:
				log.Error(line)
			case st := <-s:
				if st.Exit != 0 {
					log.Error(st.Error)
				}
				m = 1
			default:
				if m == 1 && len(iquestCmdC.Stdout) == 0 && len(iquestCmdC.Stderr) == 0 {
					break Loop
				}
			}
		}
		log.Debug("chanColls closed")
		close(chanColls)
	}()

	// spin up workers to query files in individual collections
	chanFiles := make(chan string, 2*MAX_DOWNLOAD_W)
	chanSync1 := make(chan byte)
	for i := 0; i < MAX_DOWNLOAD_W ; i++ {
		go func() {
			for {
				c, ok := <- chanColls
				if ! ok {
					break
				}

				qryf := "SELECT COLL_NAME,DATA_NAME WHERE COLL_NAME = '" + c + "'"
				log.Debug(qryf)
	 			iquestCmdF := cmd.NewCmd("iquest", "--no-page", "%s/%s", qryf)
				s := <-iquestCmdF.Start()
				for _, l := range s.Stdout {
					// this query contains no data
					if strings.Contains(l,"CAT_NO_ROWS_FOUND") {
						continue
					}
					// add 'zip' file to download, and continue with the next
					// file in the same collection.
					if isZipFile(l) {
						chanFiles <- l
						continue
					}
					// for 'tar.gz' file or 'IMA' file, we take only one file
					// from the collection.
					chanFiles <- l
					break
				}
			}
			chanSync1 <-'0'
		}()
	}

	// closing up chanFiles
	go func() {
		// blocking call to wait all workers are finished
		waitWorkers(MAX_DOWNLOAD_W, &chanSync1)
		// all workers are finished, closing the channel for querying collection files
		close(chanFiles)
	}()

	// spin up workers to download the dicom series tar.gz file in parallel
	chanDicoms := make(chan string, MAX_DOWNLOAD_W)
	chanSync2  := make(chan byte)
	for i := 0; i < MAX_DOWNLOAD_W ; i++ {
		go func() {
			for {
				f, ok := <- chanFiles
				if ! ok {
					break
				}
				fdicom, err := downloadDicom(f)
				if err != nil {
					log.Error(err)
					continue
				}
				chanDicoms <- fdicom
			}
			chanSync2 <-'0'
		}()
	}

	// closing up chanDicoms
	go func() {
		// blocking call to wait all workers are finished
		waitWorkers(MAX_DOWNLOAD_W, &chanSync2)
		// all workers are finished, closing the channel for downloading/extracting DICOM file
		close(chanDicoms)
	}()

	return chanDicoms
}

// waitWorkers waits all workers to send a "finish" signal to the chanSync channel.
// When the function receive signals from an expected number of workers, it closes up
// the chanSync channel, and returns.
func waitWorkers(nworker int, chanSync *chan byte) {
	i := 0
	for i < nworker {
		<-*chanSync
		i++
	}
	close(*chanSync)
}

// isImaFile checks wether the given path has suffix ".IMA".
func isImaFile(path string) bool {
	if filepath.Ext(path) == ".IMA" {
		return true
	}
	return false
}

// isZipFile checks wether the given path has suffix ".zip".
func isZipFile(path string) bool {
	if filepath.Ext(path) == ".zip" {
		return true
	}
	return false
}

// downloadDicom downloads a data object from iRODS, and extracts/saves one DICOM file.
// On succes, the path of the saved file is returned.
//
// This function takes into account the following situations:
//
// 1. the path refers to a 'tar.gz' archive.  In this case, one of the DICOM IMA file is
//    extracted from the archive.
//
// 2. the path refers to a DICOM 'IMA' file.  In this case, the 'IMA' file is stored as it is.
//
// 3. the path refers to a 'zip' archive.  In this case, the 'zip' file is stored as it is.
func downloadDicom(path string) (string, error) {

	// download given path from iRODS to a local directory
	i := strings.Index(path, *opts_date)
	if i < 0 {
		return "", errors.New(fmt.Sprintf("unknown path: %s",path)) 
	}
	loc := filepath.Join(*opts_ddir, *opts_date, path[i+8:])

	if isImaFile(loc) {
		// for DICOM file, we want it to be copied to the session folder, rather than
		// the series folder.
		loc = filepath.Join(filepath.Dir(filepath.Dir(loc)), filepath.Base(loc))
	}

	dir := filepath.Dir(loc)
	if err := os.MkdirAll(dir,0755); err != nil {
		return "", errors.New("cannot create dir: " + dir)
	}

	log.Debug(fmt.Sprintf("%s -> %s\n", path, loc))
	c := cmd.NewCmd("iget", "-f", path, loc)
	st := <- c.Start()
	if st.Exit != 0 {
		return "", st.Error
	}

	// the downloaded file is a zip file containg entire experiment (subject/session)
	// don't do anything; because we have all the data locally.
	if isZipFile(loc) {
		return loc, nil
	}

	// the downloaded file itself is already a DICOM file
	if isImaFile(loc) {
		// make command for downloading full dataset
		dir_src := filepath.Dir( filepath.Dir(path) )
		if err := makeDownloadCmd(dir_src, dir); err != nil {
			log.Warn(fmt.Sprintf("cannot write command: %s\n", err))
		}
		return loc, nil
	}

	// open the gizpped archive: file -> gzip -> tar
	f, err := os.Open(loc)
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
		os.Remove(loc)
	}()

	// extracted dicom filename
	var fdicom string
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return "", errors.New("empty archive: " + loc)
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

			// make command for downloading full dataset
			if err := makeDownloadCmd(filepath.Dir(path), dir); err != nil {
				log.Warn(fmt.Sprintf("cannot write command: %s\n", err))
			}
			return fdicom, nil
		}
	}
}


// copyReaderToPath copies data from the reader to a file referred by the path,
// with a given file mode.
func copyReaderToPath(reader io.Reader, path string, mode os.FileMode) error {
	fo, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, mode)
	if err != nil {
		return err
	}
	defer fo.Close()

	// copy the tar content to dicome file
	if _, err := io.Copy(fo, reader); err != nil {
		return err
	}
	return nil
}

// makeDownloadCmd creates a file `cmd.sh` in the sourceDir.  In this file, a irsync command
// is generated for synchronising data from the destDir on iRODS to the local sourceDir.
func makeDownloadCmd(sourceDir string, destDir string) error {

	// write instruction to download full directory 
	fname := filepath.Join(destDir, "cmd.sh")
	f, _  := os.OpenFile(fname, os.O_CREATE|os.O_RDWR, os.FileMode(0755))
	defer f.Close()
				
	cmd := fmt.Sprintf("irsync -r i:%s %s", sourceDir, destDir)
	_,err := f.Write([]byte(cmd))

	return err
}
