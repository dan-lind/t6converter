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
	tmpfile := writeTempFile([]byte(data1min))
	defer os.Remove(tmpfile.Name()) // clean up
	records, _ := c.FileToCsv(tmpfile.Name())
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
	tmpfile := writeTempFile([]byte(data1min))
	defer os.Remove(tmpfile.Name()) // clean up
	records, _ := c.FileToCsv(tmpfile.Name())

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
	tmpfile := writeTempFile([]byte(dailyStockData))
	defer os.Remove(tmpfile.Name()) // clean up
	records, _ := c.FileToCsv(tmpfile.Name())

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

func TestParseDailyFxToStruct(t *testing.T) {
	tmpfile := writeTempFile([]byte(dailyFxData))
	defer os.Remove(tmpfile.Name()) // clean up
	records, _ := c.FileToCsv(tmpfile.Name())

	t6records, _ := c.RwDailyToStruct(records)
	assert.Equal(t, 1, len(t6records))
	assert.Equal(t, 19, len(t6records[0]))

	parsedTime, _ := time.Parse("20060102", "20040420")
	assert.Equal(t, c.ConvertToOle(parsedTime), t6records[0][0].Date)
	assert.Equal(t, float32(2.94750000), t6records[0][0].Open)
	assert.Equal(t, float32(2.94750000), t6records[0][0].High)
	assert.Equal(t, float32(2.89110000), t6records[0][0].Low)
	assert.Equal(t, float32(2.89330000), t6records[0][0].Close)
	assert.Equal(t, float32(0), t6records[0][0].Vol)

	parsedTime2, _ := time.Parse("20060102", "20040514")
	assert.Equal(t, c.ConvertToOle(parsedTime2), t6records[0][18].Date)
	assert.Equal(t, float32(2.91840000), t6records[0][18].Open)
	assert.Equal(t, float32(2.93730000), t6records[0][18].High)
	assert.Equal(t, float32(2.90880000), t6records[0][18].Low)
	assert.Equal(t, float32(2.93190000), t6records[0][18].Close)
	assert.Equal(t, float32(0), t6records[0][18].Vol)
}

func TestCreateT6File(t *testing.T) {
	tmpfile := writeTempFile([]byte(data1min))
	defer os.Remove(tmpfile.Name()) // clean up
	records, _ := c.FileToCsv(tmpfile.Name())
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

func writeTempFile(data []byte) *os.File {
	tmpfile, err := ioutil.TempFile("test", "test")
	if err != nil {
		log.Fatal(err)
	}

	if _, err := tmpfile.Write(data); err != nil {
		log.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}
	return tmpfile
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

const dailyStockData string = `Date, Open, High, Low, Close, Volume
20010511,420.81000000,421.36000000,418.97000000,419.64000000,       0
20010514,422.37000000,422.44000000,418.72000000,419.80000000,       0
20010515,419.82000000,424.57000000,419.43000000,423.77000000,       0
20010516,423.81000000,424.61000000,421.20000000,424.61000000,       0
20010517,424.98000000,429.49000000,424.98000000,428.58000000,       0
20010518,428.58000000,431.56000000,427.89000000,429.60000000,       0
20010521,429.60000000,430.65000000,427.33000000,430.34000000,       0
20010522,430.23000000,433.39000000,429.29000000,432.27000000,       0
20010523,432.24000000,433.07000000,430.17000000,431.93000000,       0
20010524,432.24000000,433.07000000,430.17000000,431.89000000,       0
20010525,431.89000000,432.84000000,429.94000000,430.44000000,       0
20010528,430.14000000,431.19000000,428.91000000,429.51000000,       0
20010529,429.55000000,430.26000000,425.99000000,426.99000000,       0
20010530,426.99000000,428.06000000,423.49000000,424.14000000,       0
20010531,424.05000000,425.10000000,420.55000000,420.55000000,       0
20010604,420.55000000,422.96000000,419.47000000,422.94000000,       0
20010605,422.91000000,426.52000000,422.40000000,426.35000000,       0
20010606,426.37000000,426.38000000,423.60000000,425.74000000,       0
20010607,425.73000000,426.85000000,423.50000000,426.15000000,       0
`

const dailyFxData string = `Date, Open, High, Low, Close
20040420, 2.94750000, 2.94750000, 2.89110000, 2.89330000
20040421, 2.89740000, 2.91170000, 2.88480000, 2.89210000
20040422, 2.89380000, 2.90620000, 2.88240000, 2.90610000
20040423, 2.90460000, 2.90960000, 2.87750000, 2.88770000
20040426, 2.88910000, 2.91070000, 2.87310000, 2.90850000
20040427, 2.90710000, 2.93250000, 2.89920000, 2.92770000
20040428, 2.92640000, 2.93140000, 2.90110000, 2.90680000
20040429, 2.90760000, 2.94640000, 2.90080000, 2.94400000
20040430, 2.93960000, 2.94830000, 2.92080000, 2.93290000
20040503, 2.92750000, 2.93680000, 2.91660000, 2.91880000
20040504, 2.92210000, 2.96910000, 2.91690000, 2.96550000
20040505, 2.96560000, 2.99050000, 2.95290000, 2.98140000
20040506, 2.98190000, 2.98260000, 2.95470000, 2.95960000
20040507, 2.96050000, 2.97110000, 2.91420000, 2.92190000
20040510, 2.91800000, 2.92990000, 2.90540000, 2.92670000
20040511, 2.92620000, 2.93100000, 2.90330000, 2.92950000
20040512, 2.93170000, 2.95080000, 2.92050000, 2.93640000
20040513, 2.93540000, 2.94500000, 2.90420000, 2.91700000
20040514, 2.91840000, 2.93730000, 2.90880000, 2.93190000

`