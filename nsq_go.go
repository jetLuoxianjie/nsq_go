package nsq_go

import (
	"go.uber.org/zap"
)

var NsqGo *nsqGo
var Logger *zap.Logger

type nsqGo struct {
	NsqdTcpAddress       []string //Nsqd的Tcp地址
	NsqdHttpAddress      []string //Nsqd的http地址
	NsqlookupHttpAddress []string //Nsqlookup的HTTP地址
	LookupdPollInterval  int64    //消费者通过lookUp轮询查找新Nsqd的时间间隔// 也是生产者需要等多久才可以往新的Nsqd发消息的时间  (秒)

}

//1.NsqlookupHttpAddress地址  2.日志  3.消费者通过lookUp轮询查找新Nsqd的时间间隔,也是生产者需要等多久才可以往新的Nsqd发消息的时间  (秒)
func NsqGoStart(NsqlookupHttpAddress []string, logger *zap.Logger, lookupdPollInterval int64) {
	if NsqGo == nil {
		NsqGo = &nsqGo{
			NsqlookupHttpAddress: NsqlookupHttpAddress,
			LookupdPollInterval:  lookupdPollInterval,
		}
	}
	//日志赋值
	if Logger == nil {
		Logger = logger
	}
	nsqGoLogInfo("nsq_go start successful !!!!")
}



