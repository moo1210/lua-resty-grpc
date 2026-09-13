// Test gRPC backend for benchmarking lua-resty-grpc old vs new.
// Implements the grpcb.in-style hello.HelloService.
package main

import (
	"context"
	"flag"
	"io"
	"log"
	"net"
	"runtime"
	"strings"

	"google.golang.org/grpc"

	"bench/backend/hellopb"
)

type server struct {
	hellopb.UnimplementedHelloServiceServer
}

func (s *server) SayHello(ctx context.Context, req *hellopb.HelloRequest) (*hellopb.HelloReply, error) {
	return &hellopb.HelloReply{Reply: "Hello " + req.Greeting}, nil
}

func (s *server) LotsOfReplies(req *hellopb.HelloRequest, stream hellopb.HelloService_LotsOfRepliesServer) error {
	for i := 0; i < 3; i++ {
		if err := stream.Send(&hellopb.HelloReply{Reply: req.Greeting}); err != nil {
			return err
		}
	}
	return nil
}

func (s *server) LotsOfGreetings(stream hellopb.HelloService_LotsOfGreetingsServer) error {
	var sb strings.Builder
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&hellopb.HelloReply{Reply: sb.String()})
		}
		if err != nil {
			return err
		}
		sb.WriteString(req.Greeting)
	}
}

func (s *server) BidiHello(stream hellopb.HelloService_BidiHelloServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&hellopb.HelloReply{Reply: "Hello " + req.Greeting}); err != nil {
			return err
		}
	}
}

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "listen address")
	maxStreams := flag.Uint("max-streams", 1000, "MaxConcurrentStreams per connection")
	flag.Parse()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	opts := []grpc.ServerOption{
		grpc.MaxConcurrentStreams(uint32(*maxStreams)),
	}
	srv := grpc.NewServer(opts...)
	hellopb.RegisterHelloServiceServer(srv, &server{})
	log.Printf("gRPC hello backend listening on %s (GOMAXPROCS=%d, maxStreams=%d)", *addr, runtime.GOMAXPROCS(0), *maxStreams)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
