package nsq_go

import (
	"github.com/nsqio/go-nsq"
	"go.uber.org/zap"
	"testing"
)

func TestNsqGoStart(t *testing.T) {
	//1.nsqGo 启动
	logger, _ := zap.NewProduction()
	nsqlookupHttpAddress:= []string{"127.0.0.1:4161"} //nsqlookup 的地址数组
	NsqGoStart(nsqlookupHttpAddress, logger, 30, nsq.LogLevelWarning)

	//2.nsqGo 初始化生产者
	InitNsqProducer(nsq.NewConfig(), 3)

	//3.nsqGo 初始化消费者
	NewNsqConsumer("channelTest", "topicTest", OnNsqMsg, true, nsq.NewConfig())

	//4.nsqGo 生产者发消息
	NsqPush("topicTest", []byte("helloWorld"))

}

//消息Nsq消息处理
func OnNsqMsg(msg *nsq.Message) {
	nsqGoLogInfo("ok", zap.Reflect("msg", msg))
}
