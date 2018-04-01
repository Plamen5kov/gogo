package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"./helpers"
)

// how much memory do I want to use to execute sort? (distribute chuncks acordingly)
// read  chuncks from the inputFile, sort and write them into separate files
// use merge sort for generating the sorted inputFile
func main() {
	defer func() {
		fmt.Println("Main crashed with: ", recover())
	}()

	start := time.Now()
	// fileoperations.GenerateTestFile(50 /*~mb*/)
	inputFile, err := os.Open("./bigFile.txt")
	defer inputFile.Close()
	if err != nil {
		log.Fatal(err)
	}

	chunkFileSize := 5 * 1024 * 1024
	readStream := bufio.NewReaderSize(inputFile, chunkFileSize)

	fileIndex := 0
	for {
		linesRead, eof := fo.ReadNextChunck(readStream, chunkFileSize)
		sort.Strings(linesRead)
		sortedFileContent := strings.Join(linesRead, "\n")
		fo.WriteContent(strconv.Itoa(fileIndex)+".txt", sortedFileContent)

		fileIndex++
		if eof {
			break
		}
	}

	fmt.Println("Done Sorting: ", time.Since(start))
}
