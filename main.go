package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Println("Hello")

	var inputpath = "./measurements/measurements.txt"

	err := openfile(inputpath)
	if err != nil {
		fmt.Println("Error : %s", err)
	}

}

type part struct {
	offset, size int64
}

func openfile(inputpath string) ([]part, error) {
	const maxLineLength = 100
	fmt.Println(inputpath)
	var numParts = 5

	f, err := os.Open(inputpath)
	if err != nil {
		return nil, err
	}

	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := st.Size()
	splitSize := size / int64(numParts)

	fmt.Println(size)
	fmt.Println(splitSize)

	buf := make([]byte, maxLineLength)
	parts := make([]part, 0, numParts)

	return nil, nil
}
