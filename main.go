package main

import "fmt"

type Student struct {
	name string
}

func defer_test() (student *Student) {
	var student1 Student
	student1.name = "nienan"
	defer func() {
		student1.name = "sdfsdf"
		fmt.Println(student1)
	}()
	return &student1
}

func main() {
	result := defer_test()
	fmt.Println(*result)
}
