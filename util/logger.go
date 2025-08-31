package util

import (
	"log"
	"os"
)

var (
	infoLog  *log.Logger
	errorLog *log.Logger
)

func init() {
	infoLog = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	errorLog = log.New(os.Stderr, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
}

func Info(v ...interface{}) {
	infoLog.Println(v...)
}

func Error(v ...interface{}) {
	errorLog.Println(v...)
}
