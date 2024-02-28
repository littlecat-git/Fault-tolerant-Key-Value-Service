package raft
import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

//以下是为了debug而做出的努力
//.................................
func getVerbosity() int {
        v := os.Getenv("VERBOSE")
        level := 0
        if v != "" {
                var err error
                level, err = strconv.Atoi(v)
                if err != nil {
                        log.Fatalf("Invalid verbosity %v", v)
                }
        }
        return level
}

type logTopic string
const (
        dClient  logTopic = "CLNT"
        dCommit  logTopic = "CMIT"
        dDrop    logTopic = "DROP"
        dError   logTopic = "ERRO"
        dInfo    logTopic = "INFO"
        dLeader  logTopic = "LEAD"
        dLog     logTopic = "LOG1"
        dLog2    logTopic = "LOG2"
        dPersist logTopic = "PERS"
        dSnap    logTopic = "SNAP"
        dTerm    logTopic = "TERM"
        dTest    logTopic = "TEST"
        dTimer   logTopic = "TIMR"
        dTrace   logTopic = "TRCE"
        dVote    logTopic = "VOTE"
        dWarn    logTopic = "WARN"
)

var debugStart time.Time
var debugVerbosity int

func init() {
        debugVerbosity = getVerbosity()
        debugStart = time.Now()

        log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func DebugF(topic logTopic, format string, a ...interface{}) {
        if debugVerbosity >= 1 {
                time := time.Since(debugStart).Microseconds()
                time /= 100
                prefix := fmt.Sprintf("%06d %v ", time, string(topic))
                format = prefix + format
                log.Printf(format, a...)
        }
}
//以上是为了debug而做出的努力
//...................................



