package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"./helpers"
)

var pwd, _ = os.Getwd()
var sortedFilesDir = path.Join(pwd, "out")

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

	chunkFileSize := 2 * 1024 * 1024
	readStream := bufio.NewReaderSize(inputFile, chunkFileSize)
	fileIndex := 0

	for {
		linesRead, eof := fo.ReadNextChunck(readStream, chunkFileSize)
		sort.Strings(linesRead)
		sortedFileContent := strings.Join(linesRead, "\n")
		outFile := path.Join(sortedFilesDir, strconv.Itoa(fileIndex)+".txt")
		fo.WriteContent(outFile, sortedFileContent)

		fileIndex++
		if eof {
			break
		}
	}

	fmt.Println("Generated: ", fileIndex, " files in ", time.Since(start), "secconds")

	availableMemory := 50 * 1024 * 1024
	canLoadInMemory := availableMemory / chunkFileSize / 2

	//start merge sorting
	mergeSortFiles(sortedFilesDir, canLoadInMemory, chunkFileSize)

	fmt.Println("Done Sorting: ", time.Since(start))
}

func mergeSortFiles(sortedFilesDir string, canLoadInMemory int, chunkFileSize int) {

	files, err := ioutil.ReadDir(sortedFilesDir)
	if err != nil {
		log.Fatal(err)
	}
	if len(files) == 1 {
		return
	}

	loadedIndex := 0
	filesToMerge := make([]string, canLoadInMemory)

	for index, sortedChunk := range files {
		filesToMerge[loadedIndex] = path.Join(sortedFilesDir, sortedChunk.Name())
		loadedIndex++
		if loadedIndex >= canLoadInMemory || index == len(files)-1 {
			loadedIndex = 0
			mergeFiles(filesToMerge, chunkFileSize)
			filesToMerge = make([]string, canLoadInMemory)
		}
	}

	mergeSortFiles(sortedFilesDir, canLoadInMemory, chunkFileSize)
}

type streamHandler struct {
	fileHandle *os.File
	err        error
	readStream *bufio.Reader
	fileName   string
}

func mergeFiles(filePaths []string, chunkFileSize int) {
	inMemorySortedFiles := make(map[int][]string)
	openFileHandles := make(map[int]streamHandler)

	for index, currentFile := range filePaths {
		if currentFile != "" {

			//open handles on files that should be merged
			inputFileHandle, err := os.Open(currentFile)
			if err != nil {
				log.Fatal(err)
			}
			readStream := bufio.NewReader(inputFileHandle)

			// save opened handles so we can close them later on
			openFileHandles[index] = streamHandler{inputFileHandle, err, readStream, currentFile}

			// load initial file buffers
			linesRead, eof := fo.ReadNextChunck(readStream, chunkFileSize)
			inMemorySortedFiles[index] = linesRead

			if eof {
				continue
			}
		}
	}

	currentIndecies := make([]int, len(inMemorySortedFiles))
	sortedFileContentBuffer := make([]string, chunkFileSize)
	initialBufferCapacity := chunkFileSize
	bufferCounter := 0

	for {
		minElement := chooseMinElement(inMemorySortedFiles, currentIndecies)
		if minElement == "" {
			flushToFile(&sortedFileContentBuffer, chunkFileSize, &bufferCounter)
			break
		} else if len(sortedFileContentBuffer) > initialBufferCapacity {
			flushToFile(&sortedFileContentBuffer, chunkFileSize, &bufferCounter)
		}
		sortedFileContentBuffer[bufferCounter] = minElement
		bufferCounter++
	}

	// close open file handles and delete merged files
	for _, value := range openFileHandles {
		value.fileHandle.Close()
		os.Remove(value.fileName)
	}
}

func flushToFile(sortedFileContentBuffer *[]string, chunkFileSize int, bufferCounter *int) {
	outFile := path.Join(sortedFilesDir, strings.Trim(fo.GenerateRandomString(20), "\n")+".txt")
	content := strings.Trim(strings.Join(*sortedFileContentBuffer, "\n"), "\n")
	fo.WriteContent(outFile, content)
	*sortedFileContentBuffer = make([]string, chunkFileSize)
	*bufferCounter = 0
}

func chooseMinElement(inMemorySortedFiles map[int][]string, currentIndecies []int) string {
	minElement := ""
	minElementIndex := 0
	for i := 0; i < len(currentIndecies); i++ {
		currentArrayLen := len(inMemorySortedFiles[i])
		currentIndex := currentIndecies[i]
		if currentIndex < currentArrayLen {
			currentElement := inMemorySortedFiles[i][currentIndex]

			if currentElement < minElement || minElement == "" {
				minElement = currentElement
				minElementIndex = i
			}
		}
	}

	currentIndecies[minElementIndex]++
	return minElement
}
