package nsq_go

import (
	"fmt"
	"github.com/nsqio/go-nsq"
	"go.uber.org/zap"
	"sync"
	"time"
)

//消费者处理函数啊
type nsqConsumerFunc func(msg *nsq.Message)

var myNsqConsumerMgr *nsqConsumerMgr
//nsq的消息处理管理者
type nsqConsumerMgr struct {
	nsqHandlerMap map[string]nsqConsumerFunc //key 是话题 value是一个处理函数
	nsqLock       sync.RWMutex
}

//nsq消费者
type nsqConsumerHandler struct {
	topic   string //主题
	channel string //通道
}

//一个话题创建一个消费者  //创建时才建立channel通道
// 创建话题消费者  //1.服务器name  2.主题  3.话题处理handler 地址 4.是否临时一旦断开 就不发消息了 5.日志级别
func NewNsqConsumer(channel, topic string, fn nsqConsumerFunc, isEphemeral bool, conf *nsq.Config) {
	if myNsqGo == nil {
		nsqGoLogError("nsqGo not start")
		return
	}
	if conf == nil {
		nsqGoLogError("conf nil")
		return
	}
	//向每个nsq注册topic  (不注册也不影响使用，topic会在使用的时候创建，只是会些警告)
	for _, v := range myNsqGo.nsqdHttpAddress {
		address := fmt.Sprintf("http://%s/topic/create?topic=%s", v, topic)
		nsqGoHttpPost(address, nil, "application/json")
	}
	if myNsqConsumerMgr == nil{
		myNsqConsumerMgr = &nsqConsumerMgr{
			nsqHandlerMap: make(map[string]nsqConsumerFunc),
		}
	}
	//注册handler
	myNsqConsumerMgr.nsqLock.Lock()
	myNsqConsumerMgr.nsqHandlerMap[topic] = fn
	myNsqConsumerMgr.nsqLock.Unlock()
	if isEphemeral {
		channel = fmt.Sprintf("%s#ephemeral", channel)
	}
	//添加消费者
	conf.LookupdPollInterval = time.Duration(myNsqGo.lookupdPollInterval) * time.Second //设置服务发现的轮询时间,例如新的nsq出现
	//channel 是服务器名字
	c, err := nsq.NewConsumer(topic, channel, conf)
	if err != nil {
		nsqGoLogError("[nsq] newConsumer err", zap.Error(err), zap.String("serverName", channel))
		return
	}
	consumer := &nsqConsumerHandler{
		channel: channel,
		topic:   topic,
	}
	c.AddHandler(consumer)
	//设置日志
	c.SetLoggerLevel(myNsqGo.logLevel)
	//lookup地址
	addressList := myNsqGo.nsqlookupHttpAddress
	if err := c.ConnectToNSQLookupds(addressList); err != nil { // 通过nsqlookupd查询
		nsqGoLogError("[nsq] newConsumer err", zap.Error(err))
		return
	}
	nsqGoLogInfo("nsq Consumer init successfull !!! ", zap.String("serverName", channel), zap.String("topic", topic), zap.Bool("isEphemeral", isEphemeral))
	return
}

// HandleMessage 是需要实现的处理消息的方法
func (n *nsqConsumerHandler) HandleMessage(msg *nsq.Message) (err error) {
	myNsqConsumerMgr.nsqLock.RLock()
	defer myNsqConsumerMgr.nsqLock.RUnlock()
	nsqGoLogInfo("【nsq】消费者处理", zap.String("Topic", n.topic), zap.Int64("time", time.Now().Unix()))
	if fn, ok := myNsqConsumerMgr.nsqHandlerMap[n.topic]; ok {
		fn(msg)
	} else {
		nsqGoLogError("nsq topic Handler nil", zap.String("topic", n.topic))
	}
	return
}
