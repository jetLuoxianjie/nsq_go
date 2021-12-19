package nsq_go

import (
	"fmt"
	"github.com/nsqio/go-nsq"
	"go.uber.org/zap"
	"time"
)

//生产者nsq列表
var producerList []*nsq.Producer
var currIndex int //当前发消息的生产者下标

var NsqdTcpAddress []string //nsqdd Tcp地址

//初始化nsq生产者
func InitNsqProducer() {
	config := nsq.NewConfig()
	for _, address := range NsqdTcpAddress {
		producer, err := nsq.NewProducer(address, config)
		if err != nil {
			fmt.Println("create producer failed, err", zap.Error(err))
		}
		producerList = append(producerList, producer)
	}
	fmt.Println("nsqd生产者初始化完成")
}

//发送消息 //采用轮训发送
func NsqPush(topic string, data []byte) {
	//给一个nsqd发送即可,若发送失败就发送下一个
	for i := 0; i < len(producerList); i++ {
		fmt.Println("【nsq】发送消息", zap.String("Topic", topic), zap.Int64("time", time.Now().Unix()))
		currIndex++
		currIndex = currIndex % len(producerList)
		if currIndex > len(producerList)-1 {
			fmt.Println("currIndex out")
			return
		}
		producer := producerList[currIndex]
		if producer == nil {
			fmt.Println("producer nil")
			continue
		}
		err := producer.Publish(topic, data)
		if err != nil {
			fmt.Println("producer Publish err", zap.Error(err))
			continue
		}
		fmt.Println("【nsq】发送消息完成", zap.String("Topic", topic), zap.Int64("time", time.Now().Unix()))
		return
	}
	fmt.Println("all producer Publish err")
}
