package main

import (
	"errors"
	"flag"
	"fmt"
	c "github.com/dan-lind/t6converter/converters"
	"github.com/dan-lind/t6converter/model"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// A result is the product of reading and summing a file using MD5.
type result struct {
	data map[int][]model.ZorroT6
	path string
	err  error
}

//Data, Time, Open, High, Low, Close, Volume ?

func main() {

	var inputDir = flag.String("in", "", "absolute path to input directory")
	var outputDir = flag.String("out", "", "absolute path to output directory")
	var daily = flag.Bool("daily", false, "true if daily resolution")
	flag.Parse()

	start := time.Now()

	processFiles(*inputDir, *outputDir, *daily)

	fmt.Println("All done!")
	elapsed := time.Since(start)
	log.Printf("Conversion took %s", elapsed)
}

// digester reads path names from paths and sends digests of the corresponding
// files on c until either paths or done is closed.
func digester(done <-chan struct{}, paths <-chan string, res chan<- result, daily bool) {
	for path := range paths { // HLpaths
		data, err := c.FileToCsv(path)
		if err != nil {
			select {
			case res <- result{nil, path,err}:
			case <-done:
				return
			}
		}

		var records map[int][]model.ZorroT6
		var pErr error

		if daily {
			records, pErr = c.RwDailyToStruct(data)
		} else {
			records, pErr = c.Rw1minToStruct(data)
		}

		select {
		case res <- result{records, path, pErr}:
		case <-done:
			return
		}

	}
}

func processFiles(inputDir string, outputDir string, daily bool) {

	done := make(chan struct{})
	defer close(done)

	paths, errc := walkFiles(done, inputDir)

	// Start a fixed number of goroutines to read and digest files.
	res := make(chan result) // HLc
	var wg sync.WaitGroup
	const numDigesters = 8
	wg.Add(numDigesters)
	for i := 0; i < numDigesters; i++ {
		go func() {
			digester(done, paths, res, daily) // HLc
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(res) // HLc
	}()
	// End of pipeline. OMIT

	var wg2 sync.WaitGroup

	for r := range res {
		if r.err != nil {
			fmt.Println(r.err)
			return
		}
		wg2.Add(1)
		go func(input result) {
			c.StructToT6File(input.data, outputDir, strings.Split(input.path, ".")[0], daily)
			wg2.Done()
		}(r)
	}

	wg2.Wait()
	// Check whether the Walk failed.
	if err := <-errc; err != nil { // HLerrc
		fmt.Println(err)
	}

}

// walkFiles starts a goroutine to walk the directory tree at root and send the
// path of each regular file on the string channel.  It sends the result of the
// walk on the error channel.  If done is closed, walkFiles abandons its work.
func walkFiles(done <-chan struct{}, root string) (<-chan string, <-chan error) {
	paths := make(chan string)
	errc := make(chan error, 1)
	go func() { // HL
		// Close the paths channel after Walk returns.
		defer close(paths) // HL
		// No select needed for this send, since errc is buffered.
		errc <- filepath.Walk(root, func(path string, info os.FileInfo, err error) error { // HL
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return nil
			}
			if !strings.HasSuffix(strings.ToLower(info.Name()),"txt") && !strings.HasSuffix(strings.ToLower(info.Name()),"csv") {
				return nil
			}

			select {
			case paths <- path: // HL
			case <-done: // HL
				return errors.New("walk canceled")
			}
			return nil
		})
	}()
	return paths, errc
}
