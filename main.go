package main

import (
	"fmt"
	"log/slog"
	"math"
	"os"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"unsafe"
)

// =================================================
// branchless main and max copied from https://github.com/gunnarmorling/1brc/blob/98a8279669d0483b59cc40b8809e654758b5ad54/src/main/java/dev/morling/onebrc/CalculateAverage_SamuelYvon.java#L85
func max(x, y int) int {
	diff := x - y
	return x - diff*((diff>>31)&1)
}

func min(x, y int) int {
	diff := x - y
	return y + diff*((diff>>31)&1)
}

// ================================================

type Result struct {
	num int
	sum float32
	min float32
	max float32
}

// type Group struct {
// 	name   []byte
// 	result Result
// }

type Segment struct {
	offset int
	size   int
	data   []byte
	group  map[string]*Result
	// group  []Group
}

// type slot struct {
// 	occupied bool
// 	group    Group
// }

const (
	NUM_THREADS = 8 //100000 // Assumption: Goroutines are relatively cheap and we can spin up as many
	MAX_UNIQUE = 10000
	LINE_TERM = '\n'
)

func unmap(b []byte) {
	err := syscall.Munmap(b)
	if err != nil {
		slog.Error("Error ummapping: %v", err)
	}
}

func parse_data(d []byte) (string, float32) {
	// sep := bytes.IndexByte(d, ';')
	// if sep < 1 {
	// 	panic(fmt.Sprintf("; symbol not found: %q", d))
	// }

	size := len(d)
	for i := 0; i < size; i++ {
		if d[i] == ';' {
			city := d[:i]
			data := d[i+1:]
			temp, err := strconv.ParseFloat(*(*string)(unsafe.Pointer(&data)), 32)
			if err != nil {
				panic(fmt.Sprintf("parse_data strconv.Atoi error: %v", err))
			}
			return *(*string)(unsafe.Pointer(&city)), float32(math.Round(temp * 10) / 10)
		}
	}
	panic(fmt.Sprintf("; symbol not found: %q", d))

}

func process_segment(segment Segment) {
	offset := segment.offset
	size := segment.size

	if segment.offset >= 1 && segment.data[segment.offset-1] != LINE_TERM {
		i := segment.offset
		for segment.data[i] != LINE_TERM {
			i++
		}
		offset = i + 1
		size = (segment.offset + size) - offset
	}

	if segment.data[segment.offset+segment.size] != LINE_TERM {
		i := segment.offset + segment.size
		for segment.data[i] != LINE_TERM {
			i++
		}
		size = i - offset
	}

	size += 1

	off := 0
	data := segment.data[offset : offset+size]
	// result := make([]slot, 100) // Unsure size to pre-allocate

	for k := 0; k < size; k++ {
		if segment.data[offset:][k] == LINE_TERM {
			city, temp := parse_data(data[off:k])
			if result, ok := segment.group[city]; ok {
				result.max = float32(max(int(result.max * 10), int(temp * 10))) / 10
				result.min = float32(min(int(result.min * 10), int(temp * 10))) / 10
				result.num += 1
				result.sum += temp
			} else {
				segment.group[city] = &Result{
					min: temp,
					max: temp,
					num: 1,
					sum: temp,
				}
			}
			off = k + 1
		}
	}

}

func mergeSegments(segments []Segment, results map[string]*Result) {
	for _, segment := range segments {
		for city, res := range segment.group {
			if result, ok := results[city]; ok {
				result.max = float32(max(int(result.max * 10), int(res.max * 10))) / 10
				result.min = float32(min(int(result.min * 10), int(res.min * 10))) / 10
				result.num += res.num
				result.sum += res.sum
			} else {
				results[city] = &Result{
					min: res.min,
					max: res.max,
					num: res.num,
					sum: res.sum,
				}
			}
		}
	}
}

func main() {
	var waitGroup sync.WaitGroup
	file, err := os.Open("data/measurements.txt")
	if err != nil {
		slog.Error("Error opening file: %v", err)
		os.Exit(1)
	}
	defer file.Close()

	fInfo, err := file.Stat()
	if err != nil {
		slog.Error("Error getting file info: %v", err)
		os.Exit(1)
	}

	// func Mmap(fd int, offset int64, length int, prot int, flags int) (data []byte, err error)
	// Mmap(fd int, offset int64, length int, prot int, flags int) (data []byte, err error)
	fData, err := syscall.Mmap(int(file.Fd()), 0, int(fInfo.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		slog.Error("Error mapping file: %v", err)
		os.Exit(1)
	}
	defer unmap(fData)

	// smallest := 500
	// proc_count := 8

	SEGMENT_SIZE := int(fInfo.Size() / int64(NUM_THREADS))
	segments := make([]Segment, NUM_THREADS)
	dataLen := len(fData)

	size := SEGMENT_SIZE
	offset := 0
	for n := 0; n < NUM_THREADS; n++ {
		segments[n].data = fData
		segments[n].offset = offset

		if (n + 1) == NUM_THREADS {
			segments[n].size = dataLen - segments[n].offset - 1
		} else {
			segments[n].size = size
		}

		if segments[n].offset > 0 && segments[n].data[segments[n].offset-1] == LINE_TERM {
			segments[n].offset += 1
			segments[n].size -= 1
		}

		if (segments[n].offset+segments[n].size+1) < dataLen && segments[n].data[segments[n].offset+segments[n].size-1] == LINE_TERM {
			segments[n].size += 1
		}

		segments[n].group = make(map[string]*Result, 150)

		waitGroup.Add(1)
		go func(seg Segment) {
			process_segment(seg)
			waitGroup.Done()
		}(segments[n])

		size = segments[n].size
		offset = segments[n].offset + size
	}
	waitGroup.Wait()

	results := make(map[string]*Result, MAX_UNIQUE)
	mergeSegments(segments, results)

	keyList := make([]string, 0, len(results))
	for key := range results {
		keyList = append(keyList, key)
	}

	sort.Strings(keyList)

	for _, key := range keyList {
		// City=<min>/<mean>/<max>
		mean := math.Abs(float64(results[key].sum/float32(results[key].num)))
		fmt.Printf("%s=%.1f/%.1f/%.1f\n", key, results[key].min, mean, results[key].max )
	}

}
