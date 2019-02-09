package main

import (
	"flag"
	"fmt"
	c "github.com/dan-lind/t6converter/converters"
	"github.com/dan-lind/t6converter/model"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

//Data, Time, Open, High, Low, Close, Volume ?

func main() {

	var inputDir = flag.String("in", "", "absolute path to input directory")
	var outputDir = flag.String("out", "", "absolute path to output directory")
	var daily = flag.Bool("daily", false, "true if daily resolution")
	flag.Parse()

	start := time.Now()

	files, err := ioutil.ReadDir(*inputDir)
	if err != nil {
		log.Fatalf("Failed reading directory %+v", err)
	}

	processFiles(files, *inputDir, *outputDir, *daily)

	fmt.Println("All done!")
	elapsed := time.Since(start)
	log.Printf("Conversion took %s", elapsed)
}

func processFiles(files []os.FileInfo, inputDir string, outputDir string, daily bool) {
	var wg sync.WaitGroup
	for _, f := range files {

		if !f.IsDir() {
			//errorCh := make(chan error)
			wg.Add(1)
			go parseFile(f, inputDir, outputDir, daily, &wg)
			/*
			err := <-errorCh
			if err != nil {
				log.Printf("Skipping %v: %+v", f.Name(), err)
			}
			*/

		}
	}
	wg.Wait()
}

func parseFile(file os.FileInfo, inputDir string, outputDir string, daily bool, wg *sync.WaitGroup) {
	defer wg.Done()
	csvRecords, _ := c.FileToCsv(inputDir + file.Name())
	/*
	if err != nil {
		errorCh <- err
	}
	*/

	var records map[int][]model.ZorroT6
	//var pErr error

	if daily {
		records, _ = c.RwDailyToStruct(csvRecords)
	} else {
		records, _ = c.Rw1minToStruct(csvRecords)
	}

	/*
	if pErr != nil {
		errorCh <- pErr
	}
*/
	c.StructToT6File(records, outputDir, strings.Split(file.Name(),".")[0], daily)

	//errorCh <- nil
}
