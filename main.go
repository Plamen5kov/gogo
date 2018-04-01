package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

	"./helpers"
)

// how much memory do I want to use to execute sort? (distribute chuncks acordingly)
// read  chuncks from the inputFile, sort and write them into separate files
// use merge sort for generating the sorted inputFile
func main() {
	defer func() {
		fmt.Println("Main crashed with: ", recover())
	}()

	// fileoperations.GenerateTestFile(50 /*~mb*/)
	inputFile, err := os.Open("./bigFile.txt")
	defer inputFile.Close()
	if err != nil {
		log.Fatal(err)
	}

	// chunkSize := int(0.8 * float64(allowedMemory)) //maybe put 20% buffer for program operations
	chunkFileSize := 10 * 1024 * 1024
	readStream := bufio.NewReaderSize(inputFile, chunkFileSize)

	fileIndex := 0
	for {
		linesRead, eof := fo.ReadNextChunck(readStream, chunkFileSize)
		sort.Strings(linesRead)
		sortedFileContent := strings.Trim(strings.Join(linesRead, "\n"), "\n")
		fo.WriteContent(strconv.Itoa(fileIndex)+".txt", sortedFileContent)

		fileIndex++
		if eof {
			break
		}
	}

}
