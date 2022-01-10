package nsq_go

import (
	"encoding/json"
	"fmt"
	"github.com/nsqio/go-nsq"
	"go.uber.org/zap"
	"strings"
	"time"
)

//生产者nsq列表
var producerList []*NsqdProducer
var currIndex int //当前发消息的生产者下标

//记录当前所有nsq的 信息 (主要用于判断更新的nsq是否新增)
var NodesTcpMap map[string]struct{}  //k:tcp地址
var NodesHttpMap map[string]struct{} //k:http地址

//nsq节点列表
type NsqNodeList struct {
	Producers []*NsqdNode `json:"producers"`
}

//nsqd节点信息
type NsqdNode struct {
	RemoteAddress    string   `json:"remote_address"` //nsqd的远端地址(ip+端口)
	HostName         string   `json:"host_name"`
	BroadcastAddress string   `json:"broadcast_address"`
	TcpPort          int      `json:"tcp_port"`  //tcp端口
	HttpPort         int      `json:"http_port"` //http端口
	Version          string   `json:"version"`
	Topics           []string `json:"topics"`
}

//nsqd生成者
type NsqdProducer struct {
	startTime int64 //启动时间
	producer  *nsq.Producer
}

//初始化nsq生产者
func InitNsqProducer(nsqConfig *nsq.Config, updateNsqInterval int) {
	if NsqGo == nil {
		nsqGoLogError("nsqGo not start")
		return
	}
	if nsqConfig == nil {
		nsqGoLogError("nsqConfig nil")
		return
	}
	if NodesTcpMap == nil {
		NodesTcpMap = make(map[string]struct{})
	}
	if NodesHttpMap == nil {
		NodesHttpMap = make(map[string]struct{})
	}
	//更新节点信息
	updateNsqdNodes(nsqConfig)
	//定时更新
	nsqGoSetTimeout(updateNsqInterval, func(args ...interface{}) int {
		updateNsqdNodes(nsqConfig)
		return updateNsqInterval
	})
	nsqGoLogInfo("init nsq producer successful !!!!")
}

//发送消息 //采用轮训发送
func NsqPush(topic string, data []byte) bool {
	//给一个nsqd发送即可,若发送失败就发送下一个
	for i := 0; i < len(producerList); i++ {
		nsqGoLogInfo("【nsq】发送消息", zap.String("Topic", topic), zap.Int64("time", time.Now().Unix()))
		currIndex++
		currIndex = currIndex % len(producerList)
		if currIndex > len(producerList)-1 {
			nsqGoLogError("currIndex out")
			return false
		}
		producer := producerList[currIndex]
		if producer == nil {
			nsqGoLogError("producer nil")
			continue
		}
		//防止消费者还未发现新添加的nsqd
		if producer.startTime+NsqGo.LookupdPollInterval >= time.Now().Unix() {
			continue
		}
		err := producer.producer.PublishAsync(topic, data, nil)
		if err != nil {
			nsqGoLogError("nsq  PublishAsync err", zap.Error(err))
			continue
		}
		//antnet.LogInfo("【nsq】发送消息完成", zap.String("Topic", topic), zap.Int64("time", antnet.Timestamp))
		return true
	}
	nsqGoLogError("all producer Publish err", zap.Int("len", len(producerList)))
	return false
}

//更新nsqd的节点信息 （会填充到 Config.Server.NsqConfig.NsqLookUpHttpAddress Config.Server.NsqConfig.NsqdTcpAddress 中）
func updateNsqdNodes(nsqConfig *nsq.Config) {
	if nsqConfig == nil {
		nsqGoLogError("nsqConfig nil")
		return
	}
	if NsqGo == nil {
		nsqGoLogInfo("nsqGo not start")
		return
	}
	if NodesTcpMap == nil {
		NodesTcpMap = make(map[string]struct{})
	}
	if NodesHttpMap == nil {
		NodesHttpMap = make(map[string]struct{})
	}
	//增加的nsqd节点
	addNodesTcpMap := make(map[string]struct{})
	addNodesHttpMap := make(map[string]struct{})
	//当前的nsqd连接 （会删除一些没有使用的）
	currNodesTcpMap := make(map[string]struct{})
	for _, address := range NsqGo.NsqlookupHttpAddress {
		url := fmt.Sprintf("http://%v/nodes", address)
		body := nsqGoHttpGet(url)
		if len(body) == 0 {
			continue
		}
		nl := &NsqNodeList{}
		err := json.Unmarshal(body, nl)
		if err != nil {
			nsqGoLogError("Unmarshal err", zap.Error(err))
			return
		}
		for _, v := range nl.Producers {
			if v == nil {
				continue
			}
			strList := strings.Split(v.RemoteAddress, ":")
			if len(strList) != 2 {
				nsqGoLogError("RemoteAddress err", zap.String("RemoteAddress", v.RemoteAddress))
				continue
			}
			ip := strList[0]
			if ip == "127.0.0.1" {
				ip = strings.Split(address, ":")[0]
			}
			tcpAddress := fmt.Sprintf("%v:%v", ip, v.TcpPort)
			//填充tcpMap
			if _, ok := NodesTcpMap[tcpAddress]; !ok {
				NodesTcpMap[tcpAddress] = struct{}{}
				//增加Tcp
				addNodesTcpMap[tcpAddress] = struct{}{}
			}
			//记录当前的
			currNodesTcpMap[tcpAddress] = struct{}{}

			httpAddress := fmt.Sprintf("%v:%v", ip, v.HttpPort)
			//填充HttpMap
			if _, ok := NodesHttpMap[httpAddress]; !ok {
				NodesHttpMap[httpAddress] = struct{}{}
				//增加http
				addNodesHttpMap[httpAddress] = struct{}{}
			}
		}
	}
	isFirst := len(NsqGo.NsqdTcpAddress) == 0
	//填充 增加的
	for address, _ := range addNodesTcpMap {
		NsqGo.NsqdTcpAddress = append(NsqGo.NsqdTcpAddress, address)
		//增加nsqd的生产者
		conf := nsq.NewConfig()
		producer, err := nsq.NewProducer(address, conf)
		if err != nil {
			nsqGoLogError("create producer failed, err", zap.Error(err))
		}
		startTime := time.Now().Unix()
		if isFirst {
			//第一次可以立即使用
			startTime = 0
		}
		producerList = append(producerList, &NsqdProducer{
			startTime: startTime,
			producer:  producer,
		})
	}
	for address, _ := range addNodesHttpMap {
		NsqGo.NsqdHttpAddress = append(NsqGo.NsqdHttpAddress, address)
	}

	//删除没有注册的
	for i, p := range producerList {
		isDel := true
		for add, _ := range currNodesTcpMap {
			if p.producer.String() == add {
				isDel = false
				break
			}
		}
		if !isDel {
			continue
		}
		//判断下 但是基本不可能  //slice中 如: arr:=[]int{1}  arr[len(arr):]这样是合法的√   arr[len(arr)]这样才是下标越界×
		if i+1 > len(producerList) {
			nsqGoLogError("arr index out", zap.Int("i+1", i+1), zap.Int("len", len(producerList)))
			continue
		}
		//producerList 删除
		producerList = append(producerList[:i], producerList[i+1:]...)
		//NodesTcpMap 删除
		delete(NodesTcpMap, p.producer.String())
		//重置currIndex
		currIndex = 0
	}
}
