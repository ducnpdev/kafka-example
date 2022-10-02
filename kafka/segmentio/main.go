package segmentio

import (
	"context"
	"time"
)

func Main() {
	ctx := context.Background()
	//
	forever := make(chan bool, 1)
	go func() {
		MainProduce(ctx)
	}()
	time.Sleep(time.Second)
	go func() {
		MainConsume(ctx)
	}()
	<-forever

}
