package main

import (
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"./sort"
)

var pwd, _ = os.Getwd()
var outputDir = path.Join(pwd, "out")

// read  chuncks from the inputFile, sort and write them into separate files
// merge generated sorted files into one
func main() {
	defer func() {
		if err := recover(); err != nil {
			log.Fatal(err)
		}
	}()

	//TODO: get these values from a configuration file relative to main.exe
	chunkFileSize := 5 * 1024 * 1024
	availableMemory := 70 * 1024 * 1024 // the hard drive works with 35mb/s read/write so about 70mb of operational memmory should be close to optimal
	inputFilePath := "./input/bigFile.txt"

	// fileoperations.GenerateTestFile(50 /*~mb*/)

	start := time.Now()

	msort.SortInputFile(inputFilePath, outputDir, availableMemory, chunkFileSize)

	fmt.Println(time.Since(start))
}
