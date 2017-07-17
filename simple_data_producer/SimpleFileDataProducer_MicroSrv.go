/*
Purpose:
	Service to read data from a single flat file and deliver its content to client service.
Usage:
	<Service Name> <Name of file containing paths>
Author:
	Ernesto Rodriguez
	aspacsa@gmail.com
*/
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path"
	"path/filepath"
	"sync"

	pb "riff/simple_data_producer/msg_protocol/msgprotocol"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"
)

var (
	Trace     *log.Logger // Information that will be discarted
	Info      *log.Logger // Important information
	Warning   *log.Logger // Be concerned
	Error     *log.Logger // Critical problem
	wg        sync.WaitGroup
	tls       = flag.Bool("tls", false, "Connection uses TLS if true, else plain TCP")
	certFile  = flag.String("cert_file", "testdata/server1.pem", "The TLS cert file")
	keyFile   = flag.String("key_file", "testdata/server1.key", "The TLS key file")
	port      = flag.Int("port", 10000, "The server port")
	pathsFile = flag.String("paths_file", "paths.txt", "The file containing al files' paths.")
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

// server is used to implement SimpleFileDataProducerServer.
type simpleFileDataServer struct{}

/*
	Entry point of the service.
*/
func main() {
	flag.Parse()
	progName := os.Args[0]
	fmt.Printf("Micro Service: %s\n", progName)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		grpclog.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	if *tls {
		creds, err := credentials.NewServerTLSFromFile(*certFile, *keyFile)
		if err != nil {
			grpclog.Fatalf("Failed to generate credentials %v", err)
		}
		opts = []grpc.ServerOption{grpc.Creds(creds)}
	}
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterSimpleFileDataProducerServer(grpcServer, &simpleFileDataServer{})
	grpcServer.Serve(lis)

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
func process(spath string, filename string) (datalines []string) {
	Info.Println(spath)
	dir := path.Dir(spath)

	allines := make([]string, 1)
	if _, err := os.Stat(dir); err == nil {
		spath = spath + filename
		files, _ := filepath.Glob(spath)
		for _, file := range files {
			lines := read(file)
			allines = append(allines, lines...)
		}
	} else {
		Error.Printf("Invalid path '%s'.\n", dir)
	}
	return allines
}

/*
	Function to be called by clent and responsible to return data from file.
*/
func (s *simpleFileDataServer) GetFileData(ctx context.Context, dataFile *pb.DataFile) (*pb.FileData, error) {
	data := "my data"
	var id int32 = 1
	fileName := *pathsFile
	var paths []string
	paths = read(fileName)

	for _, path := range paths { //Iterate paths in file
		process(path, dataFile.FileName) //For each path call process
	}

	return &pb.FileData{Data: data, RequestId: id}, nil
}
