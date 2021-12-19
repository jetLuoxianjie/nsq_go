package nsq_go

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/nsqio/go-nsq"
	"go.uber.org/zap"
	"io/ioutil"
	"net/http"
	"sync"
	"time"
)

type NsqConsumerFunc func(msg *nsq.Message)

//nsq的消息处理
var NsqHandlerMap = map[string]NsqConsumerFunc{} //key 是话题 value是一个处理函数
var NsqLock sync.RWMutex

var NsqdHttpAddress []string      //nsqd http地址
var NsqlookupHttpAddress []string //Nsqlookup http地址
var NsqdCnt int                   //nsd的数量

//nsq消费者
type NsqConsumerHandler struct {
	Topic   string //主题
	Channel string //服务器配置名字
}

//一个话题创建一个消费者  //创建时才建立channel通道
// 创建话题消费者  //1.服务器name  2.主题  3.话题处理handler 地址 4.是否临时一旦断开 就不发消息了
func NewNsqConsumer(channel, topic string, fn NsqConsumerFunc, isEphemeral bool) {
	//向每个nsq注册topic
	for _, v := range NsqdHttpAddress {
		address := fmt.Sprintf("http://%s/topic/create?topic=%s", v, topic)
		Post(address, nil, "application/json")
	}

	//注册handler
	NsqLock.Lock()
	NsqHandlerMap[topic] = fn
	NsqLock.Unlock()
	if isEphemeral {
		channel = fmt.Sprintf("%s#ephemeral", channel)
	}
	//添加消费者
	config := nsq.NewConfig()
	//这个MaxInFlight是接受几个nsqd的节点的意思，默认是1个(实测一个延迟较高)，建议大于等于nsqd的节点数
	config.MaxInFlight = NsqdCnt + 1
	//channel 是服务器名字
	c, err := nsq.NewConsumer(topic, channel, config)
	if err != nil {
		fmt.Println("[nsq] newConsumer err", zap.Error(err), zap.String("serverName", channel))
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
	addressList := NsqlookupHttpAddress
	if err := c.ConnectToNSQLookupds(addressList); err != nil { // 通过nsqlookupd查询
		fmt.Println("[nsq] newConsumer err", zap.Error(err))
		return
	}

	fmt.Println("nsq订阅消费者初始化", zap.String("serverName", channel), zap.String("topic", topic), zap.Bool("isEphemeral", isEphemeral))
	return
}

// HandleMessage 是需要实现的处理消息的方法
func (n *NsqConsumerHandler) HandleMessage(msg *nsq.Message) (err error) {
	NsqLock.RLock()
	defer NsqLock.RUnlock()
	if fn, ok := NsqHandlerMap[n.Topic]; ok {
		fn(msg)
	} else {
		fmt.Println("nsq topic Handler nil", zap.String("topic", n.Topic))
	}
	return
}

func Post(url string, data interface{}, contentType string) string {
	// 超时时间：5秒
	client := &http.Client{Timeout: 5 * time.Second}
	jsonStr, _ := json.Marshal(data)
	resp, err := client.Post(url, contentType, bytes.NewBuffer(jsonStr))
	if err != nil {
		fmt.Println("post err", zap.Error(err))
		return ""
	}
	defer resp.Body.Close()

	result, _ := ioutil.ReadAll(resp.Body)
	return string(result)
}
