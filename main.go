package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

type temperatureStats struct {
	min, max, sum float64
	count         int64
}

var maxGoroutines int

func main() {
	var (
		goroutines = flag.Int("goroutines", 0, "num goroutines for parallel solutions (default NumCPU)")
	)
	var inputpath = "./measurements/measurements.txt"
	output := bufio.NewWriter(os.Stdout)

	maxGoroutines = *goroutines
	if maxGoroutines == 0 {
		maxGoroutines = runtime.NumCPU()
	}
	fmt.Printf("Number of goroutines %d \n", maxGoroutines)
	partsList, err := splitFileGivenTheNumberOfParts(inputpath, maxGoroutines)
	if err != nil {
		fmt.Printf("Error : %s", err)
	}

	resultsCh := make(chan map[string]temperatureStats)
	for _, part := range partsList {
		go ProcessPartOfFile(inputpath, part.offset, part.size, resultsCh)
	}

	totals := make(map[string]temperatureStats)
	for i := 0; i < len(partsList); i++ {
		result := <-resultsCh
		for station, s := range result {
			ts, ok := totals[station]
			if !ok {
				totals[station] = temperatureStats{
					min:   s.min,
					max:   s.max,
					sum:   s.sum,
					count: s.count,
				}
				continue
			}
			ts.min = min(ts.min, s.min)
			ts.max = max(ts.max, s.max)
			ts.sum += s.sum
			ts.count += s.count
			totals[station] = ts
		}
	}

	stations := make([]string, 0, len(totals))
	for station := range totals {
		stations = append(stations, station)
	}
	sort.Strings(stations)

	fmt.Fprint(output, "{")
	for i, station := range stations {
		if i > 0 {
			fmt.Fprint(output, ", ")
		}
		s := totals[station]
		mean := s.sum / float64(s.count)
		fmt.Fprintf(output, "%s=%.1f/%.1f/%.1f", station, s.min, mean, s.max)
	}
	fmt.Fprint(output, "}\n")

	output.Flush()
}

func ProcessPartOfFile(inputPath string, fileOffset, fileSize int64, resultsCh chan map[string]temperatureStats) {
	file, err := os.Open(inputPath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	_, err = file.Seek(fileOffset, io.SeekStart)
	if err != nil {
		panic(err)
	}
	f := io.LimitedReader{R: file, N: fileSize}

	stationStats := make(map[string]temperatureStats)

	scanner := bufio.NewScanner(&f)
	for scanner.Scan() {
		line := scanner.Text()
		station, tempStr, hasSemi := strings.Cut(line, ";")
		if !hasSemi {
			continue
		}

		temp, err := strconv.ParseFloat(tempStr, 64)
		if err != nil {
			panic(err)
		}

		s, ok := stationStats[station]
		if !ok {
			s.min = temp
			s.max = temp
			s.sum = temp
			s.count = 1
		} else {
			s.min = min(s.min, temp)
			s.max = max(s.max, temp)
			s.sum += temp
			s.count++
		}
		stationStats[station] = s
	}

	resultsCh <- stationStats
}

// func splitFileGivenTheNumberOfParts(inputpath string, numParts int) ([]part, error) {
// 	const maxLineLength = 100
// 	// offset is the current position in the file.
// 	offset := int64(0)
// 	// open the file
// 	fileThatIsOpened, err := os.Open(inputpath)
// 	if err != nil {
// 		return nil, err
// 	}
// 	// what is the size of the file
// 	fileInfoStats, err := fileThatIsOpened.Stat()
// 	if err != nil {
// 		return nil, err
// 	}
// 	// It gets the file's total size (in bytes).
// 	fileSize := fileInfoStats.Size()
// 	// It calculates the size of each split part.
// 	splitSize := fileSize / int64(numParts)
// 	//buf is a small buffer (100 bytes) to help find line breaks.
// 	buf := make([]byte, maxLineLength)
// 	// partsList is a list that will store all the split partsList.
// 	partsList := make([]part, 0, numParts)
// 	// This loop will iterate over the file and find the split points.
// 	for i := range numParts {
// 		// We want to avoid cutting a line in half, so we move a little back and look for a newline (\n).
// 		seekOffset := max(offset+splitSize-maxLineLength, 0)
// 	}
// }

// This struct keeps track of where a part starts (offset) and how big (size) it is.
type part struct {
	offset, size int64
}

func splitFileGivenTheNumberOfParts(inputpath string, numParts int) ([]part, error) {
	const maxLineLength = 100

	// open the file
	f, err := os.Open(inputpath)
	if err != nil {
		return nil, err
	}

	st, err := f.Stat()

	if err != nil {
		return nil, err
	}
	// It gets the file's total size (in bytes).
	size := st.Size()
	splitSize := size / int64(numParts)

	fmt.Printf("Size of the file %d \n", size)
	fmt.Printf("Split size %d \n", splitSize)
	//buf is a small buffer (100 bytes) to help find line breaks.
	buf := make([]byte, maxLineLength)
	// partsList is a list that will store all the split partsList.
	partsList := make([]part, 0, numParts)
	// offset is the current position in the file.
	offset := int64(0)
	// This loop will iterate over the file and find the split points.
	for i := range numParts {

		// If it's the last part, just take the rest of the file
		if i == numParts-1 {
			if offset < size {
				partsList = append(partsList, part{offset, size - offset})
			}
			break
		}
		// We want to avoid cutting a line in half, so we move a little back and look for a newline (\n).
		seekOffset := max(offset+splitSize-maxLineLength, 0)
		// is moving the file read position to a specific location (seekOffset) before reading data.
		_, err := f.Seek(seekOffset, io.SeekStart)
		if err != nil {
			return nil, err
		}
		// reads a fixed amount of data from the file into the buf buffer.
		n, _ := io.ReadFull(f, buf)
		// creates a new slice (chunk) that contains only the bytes that were actually read from the file.
		chunk := buf[:n]
		// finds the last newline character in the chunk.
		newline := bytes.LastIndexByte(chunk, '\n')
		if newline < 0 {
			return nil, fmt.Errorf("newline not found at offset %d", offset+splitSize-maxLineLength)
		}
		// calculates the remaining bytes in the chunk.
		remaining := len(chunk) - newline - 1
		// calculates the next offset.
		nextOffset := seekOffset + int64(len(chunk)) - int64(remaining)
		// appends the part to the partsList list.
		partsList = append(partsList, part{offset, nextOffset - offset})
		// updates the offset to the next offset.
		offset = nextOffset
	}
	return partsList, nil
}
