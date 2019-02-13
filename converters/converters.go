package converters

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"github.com/dan-lind/t6converter/model"
	"github.com/pkg/errors"
	"io/ioutil"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"
)

func Rw1minToStruct(records [][]string) (map[int][]model.ZorroT6, error) {
	//Need to add check for missing data

	var t6 model.ZorroT6
	//t6records := make([]ZorroT6,0, len(records))
	var t6records = make(map[int][]model.ZorroT6)
	for i, record := range records {

		parsedTime, err := time.Parse("2006010215:04", record[0]+record[1])
		if err != nil {
			return nil, errors.WithMessage(err, fmt.Sprintf("Failed to parse time for record %v", i, ))
		}

		t6.Date = ConvertToOle(parsedTime)
		if open, err := strconv.ParseFloat(record[2], 32); err == nil {
			t6.Open = float32(open)
		}
		if high, err := strconv.ParseFloat(record[3], 32); err == nil {
			t6.High = float32(high)
		}
		if low, err := strconv.ParseFloat(record[4], 32); err == nil {
			t6.Low = float32(low)
		}
		if close, err := strconv.ParseFloat(record[5], 32); err == nil {
			t6.Close = float32(close)
		}
		if vol, err := strconv.ParseFloat(record[6], 32); err == nil {
			t6.Vol = float32(vol)
		}

		t6records[parsedTime.Year()] = append(t6records[parsedTime.Year()], t6)
	}

	return t6records, nil

}

func RwDailyToStruct(records [][]string) (map[int][]model.ZorroT6, error) {
	//Need to add check for missing data

	var t6records []model.ZorroT6
	for i, record := range records {
		if i == 0 {
			// skip header line
			continue
		}
		var t6 model.ZorroT6
		parsedTime, err := time.Parse("20060102", record[0])
		if err != nil {
			return nil, errors.WithMessage(err, fmt.Sprintf("Failed to parse time for record %v", i, ))
		}

		t6.Date = ConvertToOle(parsedTime)
		if open, err := strconv.ParseFloat(strings.TrimSpace(record[1]), 32); err == nil {
			t6.Open = float32(open)
		}
		if high, err := strconv.ParseFloat(strings.TrimSpace(record[2]), 32); err == nil {
			t6.High = float32(high)
		}
		if low, err := strconv.ParseFloat(strings.TrimSpace(record[3]), 32); err == nil {
			t6.Low = float32(low)
		}
		if close, err := strconv.ParseFloat(strings.TrimSpace(record[4]), 32); err == nil {
			t6.Close = float32(close)
		}
		if len(record) == 6 {
			if vol, err := strconv.ParseFloat(record[5], 32); err == nil {
				t6.Vol = float32(vol)
			}
		} else {
			t6.Vol = 0
		}

		t6.Val = float32(parsedTime.Year())
		t6records = append(t6records, t6)
	}

	return map[int][]model.ZorroT6{0: t6records}, nil
}

func FileToCsv(path string) ([][]string, error) {

	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, errors.WithMessage(err, fmt.Sprintf("Unable to read file with path %v", path))
	}

	r := csv.NewReader(bytes.NewReader(data))

	records, err := r.ReadAll()
	if err != nil {
		return nil, errors.WithMessage(err, fmt.Sprintf("Unable to read records in file %v", path))
	}

	return records, nil
}

func StructToT6File(recordMap map[int][]model.ZorroT6, outputPath string, fileName string, daily bool) {
	buf := new(bytes.Buffer)

	for _, t6records := range recordMap {
		sort.Slice(t6records, func(i, j int) bool {
			return t6records[i].Date > t6records[j].Date
		})
	}

	if daily {
		records := recordMap[0]
		writeAllRecords(records, buf)
		ioutil.WriteFile(strings.Join([]string{outputPath,fileName,".t6"},""), buf.Bytes(), 0644)
	} else {
		for year, records := range recordMap {
			buf.Reset()
			writeAllRecords(records, buf)
			ioutil.WriteFile(strings.Join([]string{outputPath,fileName,"_",strconv.Itoa(year),".t6"},""), buf.Bytes(), 0644)
		}
	}
}

func writeAllRecords(records []model.ZorroT6, buf *bytes.Buffer) {
	//for _, record := range records {
		err := binary.Write(buf, binary.LittleEndian, records)
		if err != nil {
			log.Println("binary.Write failed:", err)
		}
	//}
}

func ConvertToOle(oledate time.Time) float64 {
	return float64(oledate.Unix())/(24.*60.*60.) + 25569. // 25569. = DATE(1.1.1970 00:00)
}

func ConvertFromOle(oledate float64) time.Time {
	return time.Unix(int64((oledate-25569.)*24.*60.*60.), 0).UTC(); // 25569. = DATE(1.1.1970 00:00)
}
