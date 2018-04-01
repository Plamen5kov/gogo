package asd

import (
	"strconv"
	"time"
	"net/http"
	// "strconv"
	"io/ioutil"
	"log"
	"fmt"
	"os"
	"math"
)
func main () {
	// presAge := make(map[string] int) // map init

	//exception handling 
	// excHandling(1/0)

	//pointers
	// num := 2
	// refChange(&num)
	
	//creating pointers
	// var ptrVal *int = new(int)
	// fmt.Println("ptrVal=", ptrVal, " *ptrVal=", *ptrVal)
	// *ptrVal = 123
	// fmt.Println("ptrVal=", ptrVal, " *ptrVal=", *ptrVal)

	// how to create struct and assign methods to them
	// rect1 := Rectangle{0, 50, 10, 10}
	// fmt.Println("Rectangle area is: ", rect1.width * rect1.height)
	// fmt.Println("Rectangle area is: ", rect1.area())

	//polymorphism
	// rect := Rectangle{0, 0, 20, 50}
	// circle := Circle{4}
	// fmt.Println("Rectangle area: ", rect.area())
	// fmt.Println("Circle area: ", circle.area())
	// fmt.Println("Rectangle area: ", getArea(rect))
	// fmt.Println("Circle area: ", getArea(circle))

	// OPERATIONS 
	// testString := "hello world!"
	// fmt.Println(strings.Contains(testString, "lo"))
	// fmt.Println(strings.Index(testString, "lo"))
	// fmt.Println(strings.Count(testString, "l"))
	// fmt.Println(strings.Replace(testString, "l", "1", 100))
	// values := "1,45,5,12,3123,45123"
	// fmt.Println(strings.Split(values, ","))
	// strValues := []string{"c" , "h" , "q" ,"e","f","a","a"}
	// intValues := []int {1,5,2,13,-4,-213}
	// sort.Strings(strValues)
	// sort.Ints(intValues)
	// fmt.Println(strValues)
	// fmt.Println(intValues)

	// I/O operations
	// fileName := "test.txt"
	// fileContent := "plamen5kov"
	// fmt.Println(createAndReadFile(fileName, fileContent))

	//CONVERT NUMBERS
	// intNum := 5
	// floatNum := 10.123
	// strNum := "1213"
	// strFloat := "123.4"

	// fmt.Println(float64(intNum))
	// fmt.Println(float64(floatNum))
	// strToInt, _ := strconv.ParseInt(strNum, 0, 64)
	// fmt.Println(strToInt)
	// strToFloat, _ := strconv.ParseFloat(strFloat, 64)
	// fmt.Println(strToFloat)

	// HTTP SERVER
	// http.HandleFunc("/", handler)
	// http.HandleFunc("/hi", hiHandler)
	// http.ListenAndServe(":1234", nil)

	// GO ROUTINE
	// for i := 0; i < 10; i++ {
	// 	go count(i)
	// }
	// give goroutines time to print themselves
	// time.Sleep(time.Millisecond * 11000)

	// CHANNELS  (how to pass data between go routines)
	// stringChan := make(chan string)
	// for i := 0; i < 3; i++ {
	// 	go makeDough(stringChan)
	// 	go addSauce(stringChan)
	// 	go addToppings(stringChan)
	// 	time.Sleep(time.Millisecond * 5000)
	// }
}

var pizzaNum = 0;

func makeDough (stringChan chan string) {
	pizzaNum++
	pizzaName := "Pizza #" + strconv.Itoa(pizzaNum)
	fmt.Println("Make Dough and send for Sauce")

	stringChan <- pizzaName

	time.Sleep(time.Millisecond * 10)
}

func addSauce (stringChan chan string) {
	pizza := <- stringChan
	fmt.Println("Add sauce and send ", pizza)

	stringChan <- pizza

	time.Sleep(time.Millisecond * 10)
}

func addToppings (stringChan chan string) {
	pizza := <- stringChan
	fmt.Println("Add toppings to ", pizza)

	time.Sleep(time.Millisecond * 10)
}

func count (id int) {
	for i := 0; i < 10; i++ {
		// defer func() {
			fmt.Println(id, ":", i)
		// }()
	}

	time.Sleep(time.Millisecond * 1000)
}

func handler(responceWriter http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(responceWriter, "hi user\n")
}

func hiHandler(responceWriter http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(responceWriter, "Hi back at you!\n")
}
func createAndReadFile(fileName, fileContent string) string {
	
	file, err := os.Create(fileName)
	if err != nil {log.Fatal(err)}

	file.WriteString(fileContent)
	file.Close()

	stream, err := ioutil.ReadFile(fileName)
	if err != nil { log.Fatal(err) }

	return string(stream)
}

type Shape interface {
	area() float64
}

type Circle struct {
	radius float64
}

type Rectangle struct {
	leftX float64
	topY float64
	height float64
	width float64
}

func (rect Rectangle) area() float64 {
	return rect.width * rect.height
}
func (circle Circle) area() float64 {
	return math.Pi * math.Pow(circle.radius, 2)
}

func getArea(shape Shape) float64 {
	return (shape).area()
}

func refChange(num *int) {
	*num += 2
}

func excHandling(num1 float64, num2 float64) float64 {
	defer func() {
		fmt.Println(recover())
	}()

	panic("my Exception")
	solution := num1 / num2
	return solution
}