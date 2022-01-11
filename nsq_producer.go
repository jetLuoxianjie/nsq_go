package nsq_go

import (
	"encoding/json"
	"fmt"
	"github.com/nsqio/go-nsq"
	"go.uber.org/zap"
	"strings"
	"time"
)

var nsqdMgr *NsqdMgr

//nsqd 管理
type NsqdMgr struct {
	//生产者nsq列表
	producerList []*NsqdProducer
	currIndex    int //当前发消息的生产者下标

	//记录当前所有nsq的 信息 (主要用于判断更新的nsq是否新增)
	nodesTcpMap  map[string]struct{} //k:tcp地址
	nodesHttpMap map[string]struct{} //k:http地址

}

//nsqd生成者
type NsqdProducer struct {
	startTime int64 //启动时间
	producer  *nsq.Producer
}

//nsq节点列表 (用于从 nsqlookup 获取消息 解析使用)
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

//初始化nsq生产者
func InitNsqProducer(nsqConfig *nsq.Config, updateNsqInterval int) {
	if myNsqGo == nil {
		nsqGoLogError("nsqGo not start")
		return
	}
	if nsqConfig == nil {
		nsqGoLogError("nsqConfig nil")
		return
	}
	if nsqdMgr == nil {
		nsqdMgr = &NsqdMgr{
			currIndex:    0,
			nodesTcpMap:  make(map[string]struct{}),
			nodesHttpMap: make(map[string]struct{}),
		}
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

//发送消息 //采用轮训发送    //isSync是否同步参数 isSync填写该值就是同步，不填默认异步
func NsqPush(topic string, data []byte, isSync ...interface{}) bool {
	if nsqdMgr == nil {
		nsqGoLogError("nsqdMgr nil")
		return false
	}
	lenList := len(nsqdMgr.producerList)
	//给一个nsqd发送即可,若发送失败就发送下一个
	for i := 0; i < lenList; i++ {
		nsqGoLogInfo("【nsq】发送消息", zap.String("Topic", topic), zap.Int64("time", time.Now().Unix()))
		//这次感觉使不使用原子操作都影响不大
		nsqdMgr.currIndex++
		nsqdMgr.currIndex = nsqdMgr.currIndex % lenList
		if nsqdMgr.currIndex > lenList-1 {
			nsqGoLogError("currIndex out")
			return false
		}
		nsqGoProducer := nsqdMgr.producerList[nsqdMgr.currIndex]
		if nsqGoProducer == nil {
			nsqGoLogError("nsqGoProducer nil")
			continue
		}
		if nsqGoProducer.producer == nil {
			nsqGoLogError("nsqGoProducer producer nil")
			continue
		}
		//防止消费者还未发现新添加的nsqd
		if nsqGoProducer.startTime+myNsqGo.lookupdPollInterval >= time.Now().Unix() {
			continue
		}
		if len(isSync) > 0 {
			err := nsqGoProducer.producer.Publish(topic, data)
			if err != nil {
				nsqGoLogError("nsq  PublishAsync err", zap.Error(err))
				continue
			}
		} else {
			err := nsqGoProducer.producer.PublishAsync(topic, data, nil)
			if err != nil {
				nsqGoLogError("nsq  PublishAsync err", zap.Error(err))
				continue
			}
		}
		return true
	}
	nsqGoLogError("all producer Publish err", zap.Int("len", lenList))
	return false
}

//更新nsqd的节点信息 （会填充到 Config.Server.NsqConfig.nsqlookupHttpAddress Config.Server.NsqConfig.NsqdTcpAddress 中）
func updateNsqdNodes(nsqConfig *nsq.Config) {
	if nsqConfig == nil {
		nsqGoLogError("nsqConfig nil")
		return
	}
	if myNsqGo == nil {
		nsqGoLogError("nsqGo not start")
		return
	}
	if nsqdMgr == nil {
		nsqGoLogError("nsqdMgr nil")
		return
	}
	//增加的nsqd节点
	addNodesTcpMap := make(map[string]struct{})
	addNodesHttpMap := make(map[string]struct{})
	//当前的nsqd连接 （会删除一些没有使用的）
	currNodesTcpMap := make(map[string]struct{})
	for _, address := range myNsqGo.nsqlookupHttpAddress {
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
			if _, ok := nsqdMgr.nodesTcpMap[tcpAddress]; !ok {
				nsqdMgr.nodesTcpMap[tcpAddress] = struct{}{}
				//增加Tcp
				addNodesTcpMap[tcpAddress] = struct{}{}
			}
			//记录当前的
			currNodesTcpMap[tcpAddress] = struct{}{}

			httpAddress := fmt.Sprintf("%v:%v", ip, v.HttpPort)
			//填充HttpMap
			if _, ok := nsqdMgr.nodesHttpMap[httpAddress]; !ok {
				nsqdMgr.nodesHttpMap[httpAddress] = struct{}{}
				//增加http
				addNodesHttpMap[httpAddress] = struct{}{}
			}
		}
	}
	isFirst := len(myNsqGo.nsqdTcpAddress) == 0
	//填充 增加的
	for address, _ := range addNodesTcpMap {
		myNsqGo.nsqdTcpAddress = append(myNsqGo.nsqdTcpAddress, address)
		//增加nsqd的生产者
		producer, err := nsq.NewProducer(address, nsqConfig)
		if err != nil {
			nsqGoLogError("create producer failed, err", zap.Error(err))
		}
		startTime := time.Now().Unix()
		if isFirst {
			//第一次可以立即使用
			startTime = 0
		}
		nsqdMgr.producerList = append(nsqdMgr.producerList, &NsqdProducer{
			startTime: startTime,
			producer:  producer,
		})
	}
	for address, _ := range addNodesHttpMap {
		myNsqGo.nsqdHttpAddress = append(myNsqGo.nsqdHttpAddress, address)
	}

	//删除没有注册的
	for i, p := range nsqdMgr.producerList {
		if p == nil || p.producer == nil {
			nsqGoLogError("nil", zap.Int("index", i))
			continue
		}
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
		if i+1 > len(nsqdMgr.producerList) {
			nsqGoLogError("arr index out", zap.Int("i+1", i+1), zap.Int("len", len(nsqdMgr.producerList)))
			continue
		}
		//停止
		p.producer.Stop()
		//producerList 删除
		nsqdMgr.producerList = append(nsqdMgr.producerList[:i], nsqdMgr.producerList[i+1:]...)
		//NodesTcpMap 删除
		delete(nsqdMgr.nodesTcpMap, p.producer.String())
		//重置currIndex
		nsqdMgr.currIndex = 0
	}
}
