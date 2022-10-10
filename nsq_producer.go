package nsq_go

import (
	"encoding/json"
	"fmt"
	"github.com/nsqio/go-nsq"
	"go.uber.org/zap"
	"strings"
	"sync/atomic"
	"time"
)

var nsqdManager *nsqdMgr

// nsqdMgr nsqd管理
type nsqdMgr struct {
	// 生产者nsq列表
	producerList []*nsqdProducer

	// 当前发消息的生产者下标
	currIndex int32

	// 记录当前所有nsq的 信息 (主要用于判断更新的nsq是否新增)
	//k:tcp地址
	nodesTcpMap map[string]struct{}
	//k:http地址
	nodesHttpMap map[string]struct{}
}

// nsqdProducer nsqd生成者
type nsqdProducer struct {
	// 启动时间
	startTime int64
	producer  *nsq.Producer
}

// nsqNodeList nsq节点列表，用于从 nsqlookup 获取消息 解析使用
type nsqNodeList struct {
	Producers []*nsqdNode `json:"producers"`
}

// nsqdNode nsqd节点信息
type nsqdNode struct {
	RemoteAddress    string   `json:"remote_address"` // nsqd的远端地址(ip+端口)
	HostName         string   `json:"host_name"`
	BroadcastAddress string   `json:"broadcast_address"`
	TcpPort          int      `json:"tcp_port"`
	HttpPort         int      `json:"http_port"`
	Version          string   `json:"version"`
	Topics           []string `json:"topics"`
}

// InitNsqProducer 初始化nsq生产者
func InitNsqProducer(nsqConfig *nsq.Config, updateNsqInterval int) {
	if myNsqGo == nil {
		nsqGoLogError("[nsq] not start")
		return
	}
	if nsqConfig == nil {
		nsqGoLogError("[nsq] config nil")
		return
	}
	if nsqdManager == nil {
		nsqdManager = &nsqdMgr{
			currIndex:    0,
			nodesTcpMap:  make(map[string]struct{}),
			nodesHttpMap: make(map[string]struct{}),
		}
	}
	// 更新节点信息
	updateNsqdNodes(nsqConfig)
	// 定时更新
	nsqGoSetTimeout(updateNsqInterval, func(args ...interface{}) int {
		updateNsqdNodes(nsqConfig)
		return updateNsqInterval
	})
	nsqGoLogInfo("[nsq] init producer successful")
}

// NsqPush 发送消息，采用轮训发送
// isSync是否同步参数，isSync填写该值就是同步，不填默认异步
func NsqPush(topic string, data []byte, isSync ...interface{}) bool {
	if nsqdManager == nil {
		nsqGoLogError("[nsq] manager nil")
		return false
	}
	lenList := len(nsqdManager.producerList)
	// 给一个nsqd发送即可,若发送失败就发送下一个
	for i := 0; i < lenList; i++ {
		nsqGoLogDebug("[nsq] send message", zap.String("topic", topic))
		// TODO: 这次感觉使不使用原子操作都影响不大?
		nsqdManager.currIndex++
		index := atomic.AddInt32(&nsqdManager.currIndex, 1) % int32(lenList)
		if index == 0 {
			atomic.StoreInt32(&nsqdManager.currIndex, 0)
		}
		nsqGoProducer := nsqdManager.producerList[index]
		if nsqGoProducer == nil {
			nsqGoLogError("[nsq] producer nil")
			continue
		}
		if nsqGoProducer.producer == nil {
			nsqGoLogError("[nsq] producer.producer nil")
			continue
		}
		// 防止消费者还未发现新添加的nsqd
		if nsqGoProducer.startTime+myNsqGo.nsqlookupPollInterval >= time.Now().Unix() {
			continue
		}
		if len(isSync) > 0 {
			err := nsqGoProducer.producer.Publish(topic, data)
			if err != nil {
				nsqGoLogError("[nsq] publishAsync err", zap.Error(err))
				continue
			}
		} else {
			err := nsqGoProducer.producer.PublishAsync(topic, data, nil)
			if err != nil {
				nsqGoLogError("[nsq] publishAsync err", zap.Error(err))
				continue
			}
		}
		return true
	}
	nsqGoLogError("[nsq] all producer publish err", zap.Int("len", lenList))
	return false
}

// updateNsqdNodes 更新nsqd的节点信息
// 会填充到 Config.Server.NsqConfig.nsqlookupHttpAddress,Config.Server.NsqConfig.NsqdTcpAddress 中
func updateNsqdNodes(nsqConfig *nsq.Config) {
	if nsqConfig == nil {
		nsqGoLogError("[nsq] config nil")
		return
	}
	if myNsqGo == nil {
		nsqGoLogError("[nsq] not start")
		return
	}
	if nsqdManager == nil {
		nsqGoLogError("[nsq] manager is nil")
		return
	}
	// 增加的nsqd节点
	addNodesTcpMap := make(map[string]struct{})
	addNodesHttpMap := make(map[string]struct{})
	// 当前的nsqd连接 （会删除一些没有使用的）
	currNodesTcpMap := make(map[string]struct{})
	for _, address := range myNsqGo.nsqlookupHttpAddress {
		url := fmt.Sprintf("http://%v/nodes", address)
		body := nsqGoHttpGet(url)
		if len(body) == 0 {
			continue
		}
		nl := &nsqNodeList{}
		err := json.Unmarshal(body, nl)
		if err != nil {
			nsqGoLogError("[nsq] json unmarshal err", zap.Error(err))
			return
		}
		for _, v := range nl.Producers {
			if v == nil {
				continue
			}
			strList := strings.Split(v.RemoteAddress, ":")
			if len(strList) != 2 {
				nsqGoLogError("[nsq] remoteAddress err", zap.String("remoteAddress", v.RemoteAddress))
				continue
			}
			ip := strList[0]
			if ip == "127.0.0.1" {
				ip = strings.Split(address, ":")[0]
			}
			tcpAddress := fmt.Sprintf("%v:%v", ip, v.TcpPort)
			// 填充tcpMap
			if _, ok := nsqdManager.nodesTcpMap[tcpAddress]; !ok {
				nsqdManager.nodesTcpMap[tcpAddress] = struct{}{}
				// 增加Tcp
				addNodesTcpMap[tcpAddress] = struct{}{}
			}
			// 记录当前的
			currNodesTcpMap[tcpAddress] = struct{}{}

			httpAddress := fmt.Sprintf("%v:%v", ip, v.HttpPort)
			// 填充HttpMap
			if _, ok := nsqdManager.nodesHttpMap[httpAddress]; !ok {
				nsqdManager.nodesHttpMap[httpAddress] = struct{}{}
				// 增加http
				addNodesHttpMap[httpAddress] = struct{}{}
			}
		}
	}
	isFirst := len(myNsqGo.nsqdTcpAddress) == 0
	// 填充 增加的
	for address := range addNodesTcpMap {
		myNsqGo.nsqdTcpAddress = append(myNsqGo.nsqdTcpAddress, address)
		// 增加nsqd的生产者
		producer, err := nsq.NewProducer(address, nsqConfig)
		if err != nil {
			nsqGoLogError("[nsq] create producer failed", zap.Error(err))
		}
		startTime := time.Now().Unix()
		if isFirst {
			// 第一次可以立即使用
			startTime = 0
		}
		nsqdManager.producerList = append(nsqdManager.producerList, &nsqdProducer{
			startTime: startTime,
			producer:  producer,
		})
	}
	for address := range addNodesHttpMap {
		myNsqGo.nsqdHttpAddress = append(myNsqGo.nsqdHttpAddress, address)
	}

	// 删除没有注册的
	for i := 0; i < len(nsqdManager.producerList); i++ {
		p := nsqdManager.producerList[i]
		if p == nil || p.producer == nil {
			nsqGoLogError("[nsq] producer nil", zap.Int("index", i))
			continue
		}
		isDel := true
		for add := range currNodesTcpMap {
			if p.producer.String() == add {
				isDel = false
				break
			}
		}
		if !isDel {
			continue
		}
		// 判断下 但是基本不可能
		// slice中 如: arr:=[]int{1}  arr[len(arr):]这样是合法的√
		// arr[len(arr)]这样才是下标越界×
		if i+1 > len(nsqdManager.producerList) {
			nsqGoLogError("arr index out", zap.Int("i+1", i+1), zap.Int("len", len(nsqdManager.producerList)))
			continue
		}
		// 停止
		p.producer.Stop()
		// producerList 删除
		nsqdManager.producerList = append(nsqdManager.producerList[:i], nsqdManager.producerList[i+1:]...)
		i--
		// NodesTcpMap 删除
		delete(nsqdManager.nodesTcpMap, p.producer.String())
		// 重置currIndex
		nsqdManager.currIndex = 0
	}
}

// GetNsqdNum 获得nsqd数量
func GetNsqdNum() int {
	if nsqdManager == nil {
		nsqGoLogError("[nsq] manager nil")
		return 0
	}
	return len(nsqdManager.producerList)

}
