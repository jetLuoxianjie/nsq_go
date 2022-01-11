# nsq_go

 `nsq_go`适用于集群版nsq的场景 ,内部封装了`nsq`的生产者和消费者，生产者根据`nsqlookupd`发现连接`nsqd`。
View the go-nsq [documentation](http://godoc.org/github.com/bitly/go-nsq#Config) for configuration options.

## Example

```go
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
	nsqGoLogInfo("ok",zap.Reflect("msg",msg))
}

```

# License

 MIT