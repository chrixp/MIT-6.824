package main 

import "fmt"
import "hash/fnv"

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func main() {
	fmt.Println(ihash("DDWA") % 10)
	fmt.Println(ihash("DDAW") % 10)
	fmt.Println(ihash("DW") % 10)
	
}