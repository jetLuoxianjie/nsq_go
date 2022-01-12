package nsq_go

import (
	"github.com/nsqio/go-nsq"
	"go.uber.org/zap"
)

var myNsqGo *nsqGo
var myLogger *zap.Logger

type nsqGo struct {
	nsqdTcpAddress       []string     //Nsqd的Tcp地址
	nsqdHttpAddress      []string     //Nsqd的http地址
	nsqlookupHttpAddress []string     //Nsqlookup的HTTP地址
	lookupdPollInterval  int64        //消费者通过lookUp轮询查找新Nsqd的时间间隔// 也是生产者需要等多久才可以往新的Nsqd发消息的时间  (秒)
	logLevel             nsq.LogLevel //nsq日志级别

}

//1.nsqlookupHttpAddress地址  2.日志  3.消费者通过lookUp轮询查找新Nsqd的时间间隔,也是生产者需要等多久才可以往新的Nsqd发消息的时间  (秒)
func NsqGoStart(nsqlookupHttpAddress []string, logger *zap.Logger, lookupdPollInterval int64, lvl nsq.LogLevel) {
	//日志赋值
	if myLogger == nil {
		myLogger = logger
	}
	if len(nsqlookupHttpAddress) <= 0 {
		nsqGoLogInfo("nsq_go nsqlookupHttpAddress 0")
		return
	}
	if myNsqGo == nil {
		myNsqGo = &nsqGo{
			nsqlookupHttpAddress: nsqlookupHttpAddress,
			lookupdPollInterval:  lookupdPollInterval,
			logLevel:             lvl,
		}
	}
	nsqGoLogInfo("nsq_go start successful !!!!")
}
