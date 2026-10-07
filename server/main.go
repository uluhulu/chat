package main

import (
	"context"
	"log"
	"net"
	"strconv"
	"sync"

	pb "chat/gen"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type chatServer struct {
	pb.UnimplementedProfileServiceServer
	pb.UnimplementedChatServiceServer

	mu          sync.Mutex
	nextID      int64
	subscribers map[int64]map[chan *pb.ChatMessage]struct{}
}

func newChatServer() *chatServer {
	return &chatServer{
		subscribers: make(map[int64]map[chan *pb.ChatMessage]struct{}),
	}
}

func (s *chatServer) GetProfile(
	ctx context.Context,
	req *pb.GetProfileRequest,
) (*pb.GetProfileResponse, error) {
	profile := &pb.UserProfile{
		Id:    req.GetUserId(),
		Name:  "Alex",
		Email: "alex@example.com",
	}

	return &pb.GetProfileResponse{
		Profile: profile,
	}, nil
}

func (s *chatServer) SendMessage(
	ctx context.Context,
	req *pb.SendMessageRequest,
) (*pb.ChatMessage, error) {
	if req.GetChatId() <= 0 || req.GetText() == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"chat_id and text are required",
		)
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(
			codes.Unauthenticated,
			"user-id metadata is required",
		)
	}

	values := md.Get("user-id")
	if len(values) == 0 {
		return nil, status.Error(
			codes.Unauthenticated,
			"user-id metadata is required",
		)
	}

	userID, err := strconv.ParseInt(values[0], 10, 64)
	if err != nil || userID <= 0 {
		return nil, status.Error(
			codes.Unauthenticated,
			"invalid user-id",
		)
	}

	s.mu.Lock()
	s.nextID++

	msg := &pb.ChatMessage{
		Id:     s.nextID,
		ChatId: req.GetChatId(),
		UserId: userID,
		Text:   req.GetText(),
	}

	for ch := range s.subscribers[msg.ChatId] {
		select {
		case ch <- msg:
		default:
		}
	}

	s.mu.Unlock()

	return msg, nil
}

func (s *chatServer) GetMessages(
	req *pb.GetMessagesRequest,
	stream grpc.ServerStreamingServer[pb.ChatMessage],
) error {
	if req.GetChatId() <= 0 {
		return status.Error(
			codes.InvalidArgument,
			"chat_id is required",
		)
	}

	ch := make(chan *pb.ChatMessage, 32)

	s.mu.Lock()
	if s.subscribers[req.GetChatId()] == nil {
		s.subscribers[req.GetChatId()] =
			make(map[chan *pb.ChatMessage]struct{})
	}
	s.subscribers[req.GetChatId()][ch] = struct{}{}
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.subscribers[req.GetChatId()], ch)
		s.mu.Unlock()
	}()

	for {
		select {
		case msg := <-ch:
			if err := stream.Send(msg); err != nil {
				return err
			}

		case <-stream.Context().Done():
			return nil
		}
	}
}

func main() {
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer()
	implementation := newChatServer()

	pb.RegisterProfileServiceServer(server, implementation)
	pb.RegisterChatServiceServer(server, implementation)

	log.Println("gRPC server listening on :50051")

	if err := server.Serve(listener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
