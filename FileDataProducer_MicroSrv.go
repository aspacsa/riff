/*
Purpose:
	Service to read data from flat files and deliver it to client service.
	Files in multiple paths can be read concurrently.
Usage:
	<Service Name> <Name of file containing paths>
Author:
	Ernesto Rodriguez
	aspacsa@gmail.com
*/
package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync"
)

var (
	Trace   *log.Logger // Information that will be discarted
	Info    *log.Logger // Important information
	Warning *log.Logger // Be concerned
	Error   *log.Logger // Critical problem
	wg      sync.WaitGroup
)

/*
	First function to be called.
	Used for initialization only.
*/
func init() {
	fmt.Println("Initializing...")

	Trace = log.New(ioutil.Discard, "TRACE: ", log.Ldate|log.Ltime|log.Lshortfile)
	Info = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	Warning = log.New(os.Stdout, "WARNING: ", log.Ldate|log.Ltime|log.Lshortfile)
	Error = log.New(os.Stderr, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
}

/*
	Entry point of the service.
*/
func main() {
	progName := os.Args[0]
	fmt.Printf("Micro Service: %s\n", progName)
	totArgs := len(os.Args)
	var fileName string

	if totArgs > 1 {
		fileName = os.Args[1] //Get name of paths file
		if fileName == "" {
			log.Fatalln("Must specify the name of file containing paths.")
		}
	} else {
		log.Fatalln(fmt.Sprintf("Usage: %s <name paths file>", progName))
	}

	var paths []string
	paths = read(fileName)

	cpus := runtime.NumCPU() //Get numbers of CPUs in current machine
	fmt.Printf("Total CPUs: %d\n", cpus)
	runtime.GOMAXPROCS(cpus) //Set numbers of CPUs that can be executing
	wg.Add(len(paths))       //Set numbers of routines running
	fmt.Println("Processing the following path(s):")
	for _, path := range paths { //Iterate paths in file
		go process(path) //For each path call process
	}
	wg.Wait() //Wait here until all routines report completion

	fmt.Println("Finished.")
}

/*
	Read data lines from each flat file in paths specified.
*/
func read(fileName string) (lines []string) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalln("Failed to open file: ", err)
	}

	mylines := make([]string, 1) //Create slice with minimum 1 to hold lines
	scanner := bufio.NewScanner(file)
	for scanner.Scan() { //Read each line of the file
		line := scanner.Text() //Get line
		if line != "" {        //If line is not empty then add to collection
			mylines = append(mylines, line)
		}
	}

	if err := scanner.Err(); err != nil {
		Error.Println("Error scanning file.")
	}
	file.Close()

	return mylines
}

/*
	Here we determine if the path to file is valid,
	if valid then we read all files in directory and
	read the lines in each one of them.
*/
func process(spath string) {
	defer wg.Done()

	Info.Println(spath)
	dir := path.Dir(spath)

	if _, err := os.Stat(dir); err == nil {
		files, _ := filepath.Glob(spath)
		for _, file := range files {
			fmt.Println(file)
			var lines []string
			lines = read(file)
			lines = lines
			//for _, line := range lines {
			//fmt.Println(line)
			//}
		}
	} else {
		Error.Printf("Invalid path '%s'.\n", dir)
	}
}
