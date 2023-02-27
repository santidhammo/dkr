//go:generate protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative remotesapi/remotesapi.proto
package main

import (
	"github.com/santidhammo/dkr/config"
	"github.com/santidhammo/dkr/process"
	"github.com/santidhammo/dkr/proxies"
	"github.com/santidhammo/dkr/remotesapi"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}

	log.SetOutput(os.Stdout)

	os.Args = os.Args[1:]
	command := os.Args[0]

	if command == "request-proxy" {
		mainRequestProxy()
	} else if command == "response-proxy" {
		mainResponseProxy()
	} else {
		log.Fatalln("No valid command received, should be either request-proxy or response-proxy")
	}
}

func usage() {
	println("Usage: ", os.Args[0], " request-proxy <Request Proxy Parameters> | --help")
	println("       ", os.Args[0], " response-proxy <Response Proxy Parameters> | --help")
	os.Exit(1)
}

func mainRequestProxy() {
	proxyConfig := config.GetRequestProxyConfig()
	process.ChangeWorkingDirectory(&proxyConfig.Directory)
	requestProxy, err := proxies.NewRequestProxy(proxyConfig)
	if err != nil {
		log.Fatalln("Error received while creating proxy:", err.Error())
	}
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(128 * 1024 * 1024),
	)
	remotesapi.RegisterChunkStoreServiceServer(server, requestProxy)

	listener, _ := net.Listen("tcp", ":"+strconv.FormatUint(uint64(proxyConfig.GrpcPort), 10))

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		requestProxy.Start()
		wg.Done()
	}()

	signalChannel := make(chan os.Signal)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)
	go func() {
		for range signalChannel {
			requestProxy.Close()
			wg.Done()
			break
		}
	}()

	go func() {
		err = server.Serve(listener)
		if err != nil {
			log.Fatalln("Error while serving gRPC server:", err.Error())
		}
	}()

	wg.Wait()
	log.Println("Request Proxy Halted")
}

func mainResponseProxy() {
	proxyConfig := config.GetResponseProxyConfig()
	process.ChangeWorkingDirectory(&proxyConfig.Directory)
	responseProxy, err := proxies.NewResponseProxy(proxyConfig)
	if err != nil {
		log.Fatalln("Could not create response responseProxy:", err.Error())
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		responseProxy.Start()
		wg.Done()
	}()

	signalChannel := make(chan os.Signal)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)
	for range signalChannel {
		responseProxy.Close()
		break
	}

	wg.Wait()
	log.Println("Response Proxy Halted")
}
