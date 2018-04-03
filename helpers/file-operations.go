package fo

import (
	"bufio"
	"container/list"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// GenerateTestFile will generate a "outFileName" text file with approximately the given size
// letters are from english alphabet capital and small
func GenerateTestFile(outFileName string, mb int) {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}

	filePath := path.Join(dir, outFileName)
	content := getRandText(mb)
	WriteContent(filePath, content)
}

// WriteContent will create file if it doesn't exist and will write passed content
func WriteContent(filePath string, content string) {
	// createFile(filePath)
	fileHandle, openFileErr := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	_, err := fileHandle.Write([]byte(content))
	if err != nil {
		log.Fatal(err)
	}

	openFileErr = fileHandle.Close()
	if openFileErr != nil {
		log.Fatal(openFileErr)
	}
}

func createFile(filePath string) {
	// detect if file exists
	var _, err = os.Stat(filePath)

	// create file if not exists
	if os.IsNotExist(err) {
		var file, err = os.Create(filePath)
		if err == nil {
			return
		}
		defer file.Close()
	}

	fmt.Println("==> done creating file", filePath)
}

// ReadNextChunck reads the specified "chunkSize" from the stream and returns:
// - array with read strings
// - isEndOfFile read boolean
func ReadNextChunck(readStream *bufio.Reader, chunkSize int) (*list.List, bool) {

	currentReadSize := 0
	eofReached := false

	list := list.New()
	for chunkSize > currentReadSize {
		readLine, isPrefix, err := readStream.ReadLine()
		if err != nil {
			if err == io.EOF {
				eofReached = true
				break
			}
			log.Fatal(err)
		}
		if isPrefix {
			// don't read the line if it's not whole
			for i := 0; i < len(readLine); i++ {
				readStream.UnreadRune()
			}
			break
		}
		lineLen := len(readLine)
		currentReadSize += lineLen

		list.PushBack(string(readLine))
	}

	return list, eofReached
}

// unexported methods
func getRandText(mb int) string {
	var res []string
	fileLen := (mb / 50) * 1024 * 1024

	for i := 0; i < fileLen; i++ {
		randomLength := rand.Intn(100) // ~50 bytes
		if randomLength != 0 {
			res = append(res, GenerateRandomString(randomLength))
		}
	}

	return strings.Join(res, "")
}

// GenerateRandomString returns a random string using english letters only "n" characters long
func GenerateRandomString(n int) string {
	alphabet := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	charArr := make([]rune, n+1)

	for i := 0; i < n; i++ {
		charArr[i] = alphabet[rand.Intn(len(alphabet))]
	}
	charArr[n] = '\n'

	return string(charArr)
}
