package nsq_go

import (
	"bytes"
	"encoding/json"
	"go.uber.org/zap"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

//http post请求
func nsqGoHttpPost(url string, data interface{}, contentType string) string {
	// 超时时间：5秒
	client := &http.Client{Timeout: 5 * time.Second}
	jsonStr, _ := json.Marshal(data)
	resp, err := client.Post(url, contentType, bytes.NewBuffer(jsonStr))
	if err != nil {
		nsqGoLogError("post err", zap.Error(err))
		return ""
	}
	defer resp.Body.Close()

	result, _ := ioutil.ReadAll(resp.Body)
	return string(result)
}

//http get请求
func nsqGoHttpGet(url string) []byte {
	resp, err := http.Get(url)
	defer resp.Body.Close()
	if err != nil {
		nsqGoLogError("nsqLookup get err", zap.Error(err))
		return nil
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		nsqGoLogError("get err", zap.Error(err))
		return nil
	}
	return body
}

//调试日志接口
func nsqGoLogDebug(msg string, fields ...zap.Field) {
	if myLogger == nil {
		log.Println("nsqGo myLogger nil")
		return
	}
	myLogger.Debug(msg, fields...)
}

//关键信息日志接口
func nsqGoLogInfo(msg string, fields ...zap.Field) {
	if myLogger == nil {
		log.Println("nsqGo myLogger nil")
		return
	}
	myLogger.Info(msg, fields...)
}

//警告信息日志接口
func nsqGoLogWarn(msg string, fields ...zap.Field) {
	if myLogger == nil {
		log.Println("nsqGo myLogger nil")
		return
	}
	myLogger.Warn(msg, fields...)
}

//错误信息日志接口
func nsqGoLogError(msg string, fields ...zap.Field) {
	if myLogger == nil {
		log.Println("nsqGo myLogger nil")
		return
	}
	myLogger.Error(msg, fields...)
}

func nsqGoSetTimeout(interval int, fn func(...interface{}) int, args ...interface{}) {
	if interval < 0 {
		nsqGoLogError("new timeout interval", zap.Int("interval", interval))
		return
	}
	nsqGoLogInfo("new timeout interval", zap.Int("interval", interval))

	go func() {
		var tick *time.Timer
		for interval > 0 {
			tick = time.NewTimer(time.Second * time.Duration(interval))
			select {
			case <-tick.C:
				tick.Stop()
				interval = fn(args...)
			}
		}
	}()
}
