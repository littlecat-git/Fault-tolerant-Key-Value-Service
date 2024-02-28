package kvraft
const MaxWaitChPoolSize = 20
// 初始化 KVServer
func (kv *KVServer) initWaitChPool() {
    kv.waitChPool = make(chan chan *Op, MaxWaitChPoolSize)
    for i := 0; i < MaxWaitChPoolSize; i++ {
        ch := make(chan *Op, 1)
        kv.waitChPool <- ch
    }
}

// 获取等待通道
func (kv *KVServer) getWaitCh() chan *Op {
    select {
    case ch := <-kv.waitChPool:
        return ch
    default:
        return make(chan *Op, 1)
    }
}

// 归还等待通道到池中
func (kv *KVServer) putWaitCh(ch chan *Op) {
    select {
    case kv.waitChPool <- ch:
    default:
        // 如果池已满，直接关闭通道
        close(ch)
    }
}

