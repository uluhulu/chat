package main

import (
	"context"
	"io"
	"log"
	"time"

	pb "chat/gen"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func main() {
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	profileClient := pb.NewProfileServiceClient(conn)
	chatClient := pb.NewChatServiceClient(conn)

	ctx := context.Background()

	profileResponse, err := profileClient.GetProfile(
		ctx,
		&pb.GetProfileRequest{
			UserId: 1,
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Профиль: %+v", profileResponse.GetProfile())

	stream, err := chatClient.GetMessages(
		ctx,
		&pb.GetMessagesRequest{
			ChatId: 1,
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for {
			msg, err := stream.Recv()

			if err == io.EOF {
				return
			}

			if err != nil {
				log.Println("Ошибка stream:", err)
				return
			}

			log.Printf(
				"Новое сообщение: user=%d text=%s",
				msg.GetUserId(),
				msg.GetText(),
			)
		}
	}()

	time.Sleep(time.Second)

	ctx = metadata.AppendToOutgoingContext(
		ctx,
		"user-id",
		"1",
	)

	message, err := chatClient.SendMessage(
		ctx,
		&pb.SendMessageRequest{
			ChatId: 1,
			Text:   "Привет!",
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Отправлено: %+v", message)

	time.Sleep(2 * time.Second)
}
