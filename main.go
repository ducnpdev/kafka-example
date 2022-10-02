package main

import (
	"fmt"
	"kafka-example/kafka/segmentio"
)

func init() {
	fmt.Println("main init")
}

func main() {
	fmt.Println("main main")
	segmentio.Main()
}
