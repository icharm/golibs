package golibs

import (
	"fmt"
	"log"
	"runtime"
)

type Log interface {
	INFO()
	ERROR()
	Error()
	WARN()
	DEBUG()
}

type Logger struct {
	Format string
}

func (l Logger) INFO(content string, a ...interface{}) {
	l.output("[INFO]", content, a...)
}

func (l Logger) ERROR(content string, a ...interface{}) {
	l.output("[ERROR]", content, a...)
}

func (l Logger) Error(err error) {
	l.output("[ERROR]", err.Error())
}

func (l Logger) WARN(content string, a ...interface{}) {
	l.output("[WARN]", content, a...)
}

func (l Logger) DEBUG(content string, a ...interface{}) {
	l.output("[DEBUG]", content, a...)
}

func (l Logger) output(level string, content string, a ...interface{}) {
	pc, _, _, _ := runtime.Caller(2)
	method := runtime.FuncForPC(pc).Name()
	log.Printf(fmt.Sprintf(level+":["+method+"]: "+content+" \n", a...))
}
