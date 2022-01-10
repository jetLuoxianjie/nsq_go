package nsq_go

import (
	"fmt"
	"github.com/nsqio/go-nsq"
	"go.uber.org/zap"
	"sync"
	"time"
)

type NsqConsumerFunc func(msg *nsq.Message)

//nsq的消息处理
var NsqHandlerMap = map[string]NsqConsumerFunc{} //key 是话题 value是一个处理函数
var NsqLock sync.RWMutex

//nsq消费者
type NsqConsumerHandler struct {
	Topic   string //主题
	Channel string //服务器配置名字
}

//一个话题创建一个消费者  //创建时才建立channel通道
// 创建话题消费者  //1.服务器name  2.主题  3.话题处理handler 地址 4.是否临时一旦断开 就不发消息了
func NewNsqConsumer(channel, topic string, fn NsqConsumerFunc, isEphemeral bool, conf *nsq.Config) {
	if NsqGo == nil {
		nsqGoLogError("nsqGo not start")
		return
	}
	if conf == nil {
		nsqGoLogError("conf nil")
		return
	}
	//向每个nsq注册topic  (不注册也不影响使用，topic会在使用的时候创建，只是会些警告)
	for _, v := range NsqGo.NsqdHttpAddress {
		address := fmt.Sprintf("http://%s/topic/create?topic=%s", v, topic)
		nsqGoHttpPost(address, nil, "application/json")
	}
	//注册handler
	NsqLock.Lock()
	NsqHandlerMap[topic] = fn
	NsqLock.Unlock()
	if isEphemeral {
		channel = fmt.Sprintf("%s#ephemeral", channel)
	}
	//添加消费者
	//conf := nsq.NewConfig()
	////这个MaxInFlight是接受几个nsqd的节点的意思，默认是1个(实测一个延迟较高)，建议大于等于nsqd的节点数
	//conf.MaxInFlight = len(NsqGo.NsqdTcpAddress) + 2
	conf.LookupdPollInterval = time.Duration(NsqGo.LookupdPollInterval) * time.Second //设置服务发现的轮询时间,例如新的nsq出现
	//channel 是服务器名字
	c, err := nsq.NewConsumer(topic, channel, conf)
	if err != nil {
		nsqGoLogError("[nsq] newConsumer err", zap.Error(err), zap.String("serverName", channel))
		return
	}
	consumer := &NsqConsumerHandler{
		Channel: channel,
		Topic:   topic,
	}
	c.AddHandler(consumer)
	//设置日志
	c.SetLoggerLevel(nsq.LogLevelWarning)
	//lookup地址
	addressList := NsqGo.NsqlookupHttpAddress
	if err := c.ConnectToNSQLookupds(addressList); err != nil { // 通过nsqlookupd查询
		nsqGoLogError("[nsq] newConsumer err", zap.Error(err))
		return
	}
	nsqGoLogInfo("nsq Consumer init successfull !!! ", zap.String("serverName", channel), zap.String("topic", topic), zap.Bool("isEphemeral", isEphemeral))
	return
}

// HandleMessage 是需要实现的处理消息的方法
func (n *NsqConsumerHandler) HandleMessage(msg *nsq.Message) (err error) {
	NsqLock.RLock()
	defer NsqLock.RUnlock()
	nsqGoLogInfo("【nsq】消费者处理", zap.String("Topic", n.Topic), zap.Int64("time", time.Now().Unix()))
	if fn, ok := NsqHandlerMap[n.Topic]; ok {
		fn(msg)
	} else {
		nsqGoLogError("nsq topic Handler nil", zap.String("topic", n.Topic))
	}
	return
}
