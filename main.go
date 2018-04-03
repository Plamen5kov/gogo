package main

import (
	"bufio"
	"container/list"
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
		defer func() {
			if err := recover(); err != nil {
				log.Fatal(err)
			}
		}()
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
		list, eof := fo.ReadNextChunck(readStream, chunkFileSize)

		linesRead := listToArray(list)
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

func listToArray(list *list.List) []string {
	index := 0

	linesRead := make([]string, list.Len())
	for iterator := list.Front(); iterator != nil; iterator = iterator.Next() {
		linesRead[index] = iterator.Value.(string)
		index++
	}

	return linesRead
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
	endOfFile  *bool
}

type fileHandler struct {
	element  *string
	iterator *list.Element
}

func mergeFiles(filePaths []string, chunkFileSize int) {
	inMemorySortedFiles := make(map[int]*list.List)
	openFileHandles := make(map[int]streamHandler)

	comparedTuppleElements := make(map[int]fileHandler)
	for index, currentFile := range filePaths {
		if currentFile != "" {

			//open handles on files that should be merged
			inputFileHandle, err := os.Open(currentFile)
			if err != nil {
				log.Fatal(err)
			}
			readStream := bufio.NewReader(inputFileHandle)

			// load initial file buffers
			list, eof := fo.ReadNextChunck(readStream, chunkFileSize)
			inMemorySortedFiles[index] = list
			firstElement := list.Front().Value.(string)
			comparedTuppleElements[index] = fileHandler{&firstElement, list.Front()}
			// save opened handles so we can close them later on
			openFileHandles[index] = streamHandler{inputFileHandle, err, readStream, currentFile, &eof}
			if eof {
				continue
			}
		}
	}

	sortedFileContentBuffer := make([]string, chunkFileSize)
	bufferCounter := 0

	for {
		minElement, filesEndReached := chooseMinElement(&inMemorySortedFiles, &comparedTuppleElements, &openFileHandles, chunkFileSize)
		if minElement == "" || filesEndReached {
			flushToFile(&sortedFileContentBuffer, chunkFileSize, &bufferCounter)
			break
		} else if bufferCounter >= chunkFileSize {
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

func chooseMinElement(inMemorySortedFiles *map[int]*list.List, comparedTuppleElements *map[int]fileHandler, openFileHandles *map[int]streamHandler, chunkFileSize int) (string, bool) {
	minElement := ""
	minFileIndex := 0
	filesEndReached := true
	for currentFileIndex, currentValue := range *comparedTuppleElements {

		// if element is empty try to lazy load more values from file
		if *currentValue.element == "" {
			list, eofReached := fo.ReadNextChunck((*openFileHandles)[currentFileIndex].readStream, chunkFileSize)
			if eofReached && list.Len() <= 0 {
				continue
			}

			nextElement := list.Front()
			if nextElement != nil {
				*(*comparedTuppleElements)[currentFileIndex].iterator = *nextElement
				*(*comparedTuppleElements)[currentFileIndex].element = nextElement.Value.(string)
			} else {
				*(*comparedTuppleElements)[currentFileIndex].element = ""
			}
		}

		if *(*comparedTuppleElements)[currentFileIndex].element != "" {
			filesEndReached = false
			if *currentValue.element < minElement || minElement == "" {
				minElement = *currentValue.element
				minFileIndex = currentFileIndex
			}
		}
	}

	nextElement := (*comparedTuppleElements)[minFileIndex].iterator.Next()
	if nextElement != nil {
		*(*comparedTuppleElements)[minFileIndex].iterator = *nextElement
		*(*comparedTuppleElements)[minFileIndex].element = nextElement.Value.(string)
	} else {
		*(*comparedTuppleElements)[minFileIndex].element = ""
	}
	return minElement, filesEndReached
}
