package main

import (
	"bytes"
	"encoding/binary"
	c "github.com/dan-lind/t6converter/converters"
	"github.com/dan-lind/t6converter/model"
	"github.com/stretchr/testify/assert"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func BenchmarkParseCsvToT6(t *testing.B) {
	files, _ := ioutil.ReadDir("test/")

	for i := 0; i < t.N; i++ {
		processFiles(files, "test/", "test/", false)
	}

	newFiles, _ := ioutil.ReadDir("test/")
	for _, newFile := range newFiles {

		if filepath.Ext(newFile.Name()) == ".t6" {
			os.Remove("test/" + newFile.Name())
		}
	}

}

func BenchmarkParse1minToStruct(t *testing.B) {
	records, _ := c.FileToCsv("test/perf.csv")

	for i := 0; i < t.N; i++ {
		c.Rw1minToStruct(records)
	}
}

func TestProcessFiles(t *testing.T) {
	files, _ := ioutil.ReadDir("test/")

	processFiles(files, "test/", "test/", false)

	newFiles, _ := ioutil.ReadDir("test/")
	for _, newFile := range newFiles {

		if filepath.Ext(newFile.Name()) == ".t6" {
			os.Remove("test/" + newFile.Name())
		}
	}

}

func TestFileToCsv(t *testing.T) {
	records, _ := c.FileToCsv("test/1min.csv")
	assert.Equal(t, 20, len(records))

	assert.Equal(t, "20140102", records[0][0])
	assert.Equal(t, "09:30", records[0][1])
	assert.Equal(t, "38.88", records[0][2])
	assert.Equal(t, "38.88", records[0][3])
	assert.Equal(t, "38.82", records[0][4])
	assert.Equal(t, "38.85", records[0][5])
	assert.Equal(t, "67004", records[0][6])

	assert.Equal(t, "20150102", records[19][0])
	assert.Equal(t, "09:49", records[19][1])
	assert.Equal(t, "38.59", records[19][2])
	assert.Equal(t, "38.61", records[19][3])
	assert.Equal(t, "38.59", records[19][4])
	assert.Equal(t, "38.61", records[19][5])
	assert.Equal(t, "1762", records[19][6])
}

func TestParse1minToStruct(t *testing.T) {
	records, _ := c.FileToCsv("test/1min.csv")

	t6records, _ := c.Rw1minToStruct(records)
	assert.Equal(t, 2, len(t6records))
	assert.Equal(t, 18, len(t6records[2014]))
	assert.Equal(t, 2, len(t6records[2015]))

	parsedTime, _ := time.Parse("200601021504", "201401020930")
	assert.Equal(t, c.ConvertToOle(parsedTime), t6records[2014][0].Date)
	assert.Equal(t, float32(38.88), t6records[2014][0].Open)
	assert.Equal(t, float32(38.88), t6records[2014][0].High)
	assert.Equal(t, float32(38.82), t6records[2014][0].Low)
	assert.Equal(t, float32(38.85), t6records[2014][0].Close)
	assert.Equal(t, float32(67004), t6records[2014][0].Vol)

	parsedTime2, _ := time.Parse("200601021504", "201501020949")
	assert.Equal(t, c.ConvertToOle(parsedTime2), t6records[2015][1].Date)
	assert.Equal(t, float32(38.59), t6records[2015][1].Open)
	assert.Equal(t, float32(38.61), t6records[2015][1].High)
	assert.Equal(t, float32(38.59), t6records[2015][1].Low)
	assert.Equal(t, float32(38.61), t6records[2015][1].Close)
	assert.Equal(t, float32(1762), t6records[2015][1].Vol)

}

func TestParseDailyToStruct(t *testing.T) {
	records, _ := c.FileToCsv("test/daily/daily.csv")

	t6records, _ := c.RwDailyToStruct(records)
	assert.Equal(t, 1, len(t6records))
	assert.Equal(t, 19, len(t6records[0]))

	parsedTime, _ := time.Parse("20060102", "20010511")
	assert.Equal(t, c.ConvertToOle(parsedTime), t6records[0][0].Date)
	assert.Equal(t, float32(420.81), t6records[0][0].Open)
	assert.Equal(t, float32(421.36), t6records[0][0].High)
	assert.Equal(t, float32(418.97), t6records[0][0].Low)
	assert.Equal(t, float32(419.64), t6records[0][0].Close)
	assert.Equal(t, float32(0), t6records[0][0].Vol)

	parsedTime2, _ := time.Parse("20060102", "20010607")
	assert.Equal(t, c.ConvertToOle(parsedTime2), t6records[0][18].Date)
	assert.Equal(t, float32(425.73), t6records[0][18].Open)
	assert.Equal(t, float32(426.85), t6records[0][18].High)
	assert.Equal(t, float32(423.50), t6records[0][18].Low)
	assert.Equal(t, float32(426.15), t6records[0][18].Close)
	assert.Equal(t, float32(0), t6records[0][18].Vol)
}

func TestCreateT6File(t *testing.T) {
	records, _ := c.FileToCsv("test/1min.csv")
	t6records, _ := c.Rw1minToStruct(records)

	c.StructToT6File(t6records, "test/", "1min", false)

	t6FromFile := readt6("test/1min_2014.t6")

	assert.Equal(t, 18, len(t6FromFile))

	parsedTime, _ := time.Parse("200601021504", "201401020947")

	assert.Equal(t, parsedTime, c.ConvertFromOle(t6FromFile[0].Date))
	assert.Equal(t, float32(38.67), t6FromFile[0].Open)
	assert.Equal(t, float32(38.68), t6FromFile[0].High)
	assert.Equal(t, float32(38.61), t6FromFile[0].Low)
	assert.Equal(t, float32(38.61), t6FromFile[0].Close)
	assert.Equal(t, float32(7784), t6FromFile[0].Vol)

	t6FromFile2 := readt6("test/1min_2015.t6")

	assert.Equal(t, 2, len(t6FromFile2))
	parsedTime2, _ := time.Parse("200601021504", "201501020949")
	assert.Equal(t, parsedTime2, c.ConvertFromOle(t6FromFile2[0].Date))
	assert.Equal(t, float32(38.59), t6FromFile2[0].Open)
	assert.Equal(t, float32(38.61), t6FromFile2[0].High)
	assert.Equal(t, float32(38.59), t6FromFile2[0].Low)
	assert.Equal(t, float32(38.61), t6FromFile2[0].Close)
	assert.Equal(t, float32(1762), t6FromFile2[0].Vol)

	os.Remove("test/1min_2014.t6")
	os.Remove("test/1min_2015.t6")

}

func readt6(path string) []model.ZorroT6 {

	file, err := os.Open(path)
	if err != nil {
		log.Fatal("Error while opening file", err)
	}

	defer file.Close()

	var records []model.ZorroT6
	sto := model.ZorroT6{}

	for {
		data, err := readNextBytes(file, 32) // 6 * float32 = 32
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal("buffer.Read failed", err)
		}

		buffer := bytes.NewBuffer(data)
		err = binary.Read(buffer, binary.LittleEndian, &sto)
		if err != nil {
			log.Fatal("binary.Read failed", err)
		}

		records = append(records, sto)
	}

	return records

}

func readNextBytes(file *os.File, number int) ([]byte, error) {
	bytes := make([]byte, number)

	_, err := file.Read(bytes)

	return bytes, err
}

const data1min string = `20140102,09:30,38.88,38.88,38.82,38.85,67004
20140102,09:31,38.88,38.88,38.82,38.82,2805
20140102,09:32,38.78,38.81,38.78,38.81,3380
20140102,09:33,38.81,38.84,38.78,38.83,12083
20140102,09:34,38.82,38.82,38.81,38.81,1320
20140102,09:35,38.8,38.83,38.8,38.83,3067
20140102,09:36,38.8,38.82,38.75,38.75,2791
20140102,09:37,38.76,38.76,38.71,38.73,6621
20140102,09:38,38.71,38.71,38.64,38.64,3772
20140102,09:39,38.63,38.65,38.61,38.63,11631
20140102,09:40,38.62,38.62,38.54,38.55,3211
20140102,09:41,38.54,38.54,38.51,38.51,4700
20140102,09:42,38.51,38.6,38.51,38.59,8859
20140102,09:43,38.6,38.63,38.6,38.63,1649
20140102,09:44,38.63,38.63,38.58,38.58,6609
20140102,09:45,38.59,38.63,38.59,38.62,9614
20140102,09:46,38.65,38.65,38.65,38.65,1304
20140102,09:47,38.67,38.68,38.61,38.61,7784
20150102,09:48,38.61,38.65,38.6,38.6,13938
20150102,09:49,38.59,38.61,38.59,38.61,1762
`
