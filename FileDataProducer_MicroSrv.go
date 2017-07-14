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

func init() {
	fmt.Println("Initializing...")

	Trace = log.New(ioutil.Discard, "TRACE: ", log.Ldate|log.Ltime|log.Lshortfile)
	Info = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	Warning = log.New(os.Stdout, "WARNING: ", log.Ldate|log.Ltime|log.Lshortfile)
	Error = log.New(os.Stderr, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
}

func main() {
	progName := os.Args[0]
	fmt.Printf("Micro Service: %s\n", progName)
	totArgs := len(os.Args)
	var fileName string

	if totArgs > 1 {
		fileName = os.Args[1]
		if fileName == "" {
			log.Fatalln("Must specify the name of file containing paths.")
		}
	} else {
		log.Fatalln(fmt.Sprintf("Usage: %s <name paths file>", progName))
	}

	var paths []string
	paths = read(fileName)

	cpus := runtime.NumCPU()
	fmt.Printf("Total CPUs: %d\n", cpus)
	runtime.GOMAXPROCS(cpus)
	wg.Add(len(paths))
	fmt.Println("Processing the following path(s):")
	for _, path := range paths {
		go process(path)
	}
	wg.Wait()

	fmt.Println("Finished.")
}

func read(fileName string) (lines []string) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalln("Failed to open file: ", err)
	}

	mylines := make([]string, 1)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			mylines = append(mylines, line)
		}
	}

	if err := scanner.Err(); err != nil {
		Error.Println("Error scanning file.")
	}
	file.Close()

	return mylines
}

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
