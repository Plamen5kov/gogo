package msort

import (
	"bufio"
	"container/list"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"../helpers"
)

// SortInputFile sorts a large text file
// params	inputFilePath: path to large input file to sort
// 			availableMemory: aproximately how much memory will the sort use
//			chunkFileSize: these are the chunks read from the large input file
func SortInputFile(inputFilePath string, outputDir string, availableMemory int, chunkFileSize int) {
	canLoadInMemory := availableMemory / chunkFileSize / 2
	inputFile, err := os.Open(inputFilePath)
	defer inputFile.Close()
	if err != nil {
		log.Fatal(err)
	}

	sortFileChunks(inputFile, outputDir, chunkFileSize)

	mergeSortedFiles(outputDir, canLoadInMemory, chunkFileSize)
}

func sortFileChunks(inputFile *os.File, outputDir string, chunkFileSize int) {
	readStream := bufio.NewReaderSize(inputFile, chunkFileSize)
	fileIndex := 0
	for {
		list, eof := fo.ReadNextChunck(readStream, chunkFileSize)

		linesRead := listToArray(list)
		sort.Strings(linesRead)
		sortedFileContent := strings.Join(linesRead, "\n")
		outFile := path.Join(outputDir, strconv.Itoa(fileIndex)+".txt")
		fo.WriteContent(outFile, sortedFileContent)

		fileIndex++
		if eof {
			break
		}
	}
}

func mergeSortedFiles(outputDir string, canLoadInMemory int, chunkFileSize int) {

	files, err := ioutil.ReadDir(outputDir)
	if err != nil {
		log.Fatal(err)
	}
	if len(files) == 1 {
		return
	}

	loadedFilesIndex := 0
	filesToMerge := make([]string, canLoadInMemory)

	for index, sortedChunk := range files {
		filesToMerge[loadedFilesIndex] = path.Join(outputDir, sortedChunk.Name())
		loadedFilesIndex++
		if loadedFilesIndex >= canLoadInMemory || index == len(files)-1 {
			loadedFilesIndex = 0
			mergeFiles(filesToMerge, chunkFileSize, outputDir)
			filesToMerge = make([]string, canLoadInMemory)
		}
	}

	mergeSortedFiles(outputDir, canLoadInMemory, chunkFileSize)
}

type streamHandler struct {
	fileHandle *os.File
	readStream *bufio.Reader
	fileName   string
}

type fileCompareHandler struct {
	element  *string
	iterator *list.Element
}

func mergeFiles(filePaths []string, chunkFileSize int, outputDir string) {
	inMemorySortedFiles := make(map[int]*list.List)
	openFileHandles := make(map[int]streamHandler)

	comparedTupleElements := make(map[int]fileCompareHandler)
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

			// save opened handles so we can close them later on
			comparedTupleElements[index] = fileCompareHandler{&firstElement, list.Front()}
			openFileHandles[index] = streamHandler{inputFileHandle, readStream, currentFile}
			if eof {
				continue
			}
		}
	}

	sortedFileContentBuffer := make([]string, chunkFileSize)
	bufferCounter := 0

	for {
		minElement, filesEndReached := chooseMinElement(&inMemorySortedFiles, &comparedTupleElements, &openFileHandles, chunkFileSize)
		if minElement == "" || filesEndReached {
			flushToFile(&sortedFileContentBuffer, chunkFileSize, &bufferCounter, outputDir)
			break
		} else if bufferCounter >= chunkFileSize {
			flushToFile(&sortedFileContentBuffer, chunkFileSize, &bufferCounter, outputDir)
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

func flushToFile(sortedFileContentBuffer *[]string, chunkFileSize int, bufferCounter *int, outputDir string) {
	outFile := path.Join(outputDir, strings.Trim(fo.GenerateRandomString(20), "\n")+".txt")
	content := strings.Trim(strings.Join(*sortedFileContentBuffer, "\n"), "\n")
	fo.WriteContent(outFile, content)
	*sortedFileContentBuffer = make([]string, chunkFileSize)
	*bufferCounter = 0
}

func chooseMinElement(inMemorySortedFiles *map[int]*list.List, comparedTupleElements *map[int]fileCompareHandler, openFileHandles *map[int]streamHandler, chunkFileSize int) (string, bool) {
	minElement := ""
	minFileIndex := 0
	filesEndReached := true
	for currentFileIndex, currentValue := range *comparedTupleElements {

		// if current element is empty try to lazy load more values from file
		if *currentValue.element == "" {
			list, eofReached := fo.ReadNextChunck((*openFileHandles)[currentFileIndex].readStream, chunkFileSize)
			if eofReached && list.Len() <= 0 {
				continue
			}

			*(*comparedTupleElements)[currentFileIndex].iterator = *list.Front()
			pushNextElementToTuple(comparedTupleElements, currentFileIndex)
		}

		if *(*comparedTupleElements)[currentFileIndex].element != "" {
			filesEndReached = false
			if *currentValue.element < minElement || minElement == "" {
				minElement = *currentValue.element
				minFileIndex = currentFileIndex
			}
		}
	}

	pushNextElementToTuple(comparedTupleElements, minFileIndex)
	return minElement, filesEndReached
}

func pushNextElementToTuple(comparedTupleElements *map[int]fileCompareHandler, index int) {
	nextElement := (*comparedTupleElements)[index].iterator.Next()
	if nextElement != nil {
		*(*comparedTupleElements)[index].iterator = *nextElement
		*(*comparedTupleElements)[index].element = nextElement.Value.(string)
	} else {
		*(*comparedTupleElements)[index].element = ""
	}
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
