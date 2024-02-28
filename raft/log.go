package raft
import (
	//"fmt"
)

type logEntry struct{
	Term int
	Command interface{}
}

type Log struct{
	entry []logEntry
	index0 int
}

func mkLogEmpty() Log{
	return Log{make([]logEntry, 1), 0}
}



func mkLog(log []logEntry, index0 int)Log{
	return Log{log, index0}
}

func (l *Log) append(e logEntry){
	l.entry = append(l.entry, e)
}

func (l *Log) start() int{
	return l.index0
}

//该位置及该位置之后的切片全都丢弃
func (l *Log) cutEnd(index int ){
	l.entry = l.entry[0: index-l.index0]
}

//只保留该位置及该位置之后的切片
/*func (l *Log) cutStart(index int){
	l.index0 += index
	l.entry = l.entry[index:]
}*/

func (l *Log) cutStart(index int){
	temp := index - l.index0
	l.index0 = index
	l.entry = l.entry[temp:]
}

//返回该位置及其之后的切片
func (l *Log) slice(index int) []logEntry{
	return l.entry[index-l.index0:]
}

func (l *Log) lastIndex() int{
	return l.index0 + len(l.entry) - 1
}

/*func (l *Log) getEntry(index int) *logEntry{
	return &(l.entry[index - l.index0])
}*/


func (l *Log) getEntry(index int, who int, callerInfo string) *logEntry {
    // 添加边界检查
    if index < l.index0 || index >= l.index0+len(l.entry) {
	DebugF(dError, "S%d, getEntry error,args:%d, whe: %v, sta:%d, len:%d", who, index, callerInfo, l.start(), len(l.entry))
	return nil
    }

    return &(l.entry[index-l.index0])
}

/*
func (l *Log) lastEntry() *logEntry{
	return l.getEntry(l.lastIndex())
}*/



