# nsq_go

### NSQ 简介

`NSQ`是Go语言编写的一个开源的实时分布式内存消息队列，其性能十分优异。

`NSQ` 是实时的分布式消息处理平台，其设计的目的是用来大规模地处理每天数以十亿计级别的消息。它具有分布式和去中心化拓扑结构，该结构具有无单点故障、故障容错、高可用性以及能够保证消息的可靠传递的特征。



##### 生产者

``` stylus
//nsq生产者初始化
InitNsqProducer()
```

``` stylus
//发送消息
	NsqPush(话题, 数据)
```

#### 消费者

``` ebnf
//一个话题创建一个消费者  //创建时才建立channel通道
// 创建话题消费者  //1.服务器name  2.主题  3.话题处理handler 地址 4.是否临时一旦断开 就不发消息了
func NewNsqConsumer(channel, topic string, fn NsqConsumerFunc, isEphemeral bool) {
```

