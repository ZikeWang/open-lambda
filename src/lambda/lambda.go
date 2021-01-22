package lambda

import (
	"bufio"
	"container/list"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/open-lambda/open-lambda/ol/common"
	"github.com/open-lambda/open-lambda/ol/sandbox"
)

// provides thread-safe getting of lambda functions and collects all
// lambda subsystems (resource pullers and sandbox pools) in one place
type LambdaMgr struct {
	// subsystems (these are thread safe)
	sbPool sandbox.SandboxPool
	*DepTracer
	*PackagePuller // depends on sbPool and DepTracer
	*ImportCache   // depends PackagePuller
	*HandlerPuller // depends on sbPool and ImportCache[optional]

	// storage dirs that we manage
	codeDirs    *common.DirMaker
	scratchDirs *common.DirMaker

	codeDir string

	// thread-safe map from a lambda's name to its LambdaFunc
	mapMutex sync.Mutex
	lfuncMap map[string]*LambdaFunc

	// 用于维护 docker-pool 的数据结构
	sbMapMutex sync.Mutex // 对 sbMap 的操作加锁
	sbMap   map[int]*SbMeta // map 记录了从 “容器ID” 到 “容器状态结构体” 的映射
	cap     int // docker-pool 的容量，也即当前最大容器编号，若新增容器则从该编号递增

	lAMutex sync.Mutex // 对 lActive 的操作加锁
	lActive *list.List // 处于 active&空闲 状态的容器的ID列表

	lPMutex sync.Mutex // 对 lPause 的操作加锁
	lPause  *list.List // 处于 paused&空闲 状态的容器的ID列表

	//lRun    *list.List // 处于 正在执行任务 状态的容器的ID列表
	lEmpty   *list.List // 当容器被销毁后空出来的ID列表，复用ID时从该列表选取编号

	killWarmChan chan bool // 经 mgr.KillPreWarmer() 发送 true 信号后, mgr.PreWarmer() 接收信号跳出 for 循环
}

// 代表单个容器的状态信息集合
type SbMeta struct {
	id         int             // 容器编号
	sb         sandbox.Sandbox // 容器实际对象
	scratchDir string          // 容器内 /host 映射到本地的路径
}

// Represents a single lambda function (the code)
type LambdaFunc struct {
	lmgr *LambdaMgr
	name string

	// lambda code
	lastPull *time.Time
	codeDir  string
	meta     *sandbox.SandboxMeta

	// lambda execution
	funcChan  chan *Invocation // server to func
	instChan  chan *Invocation // func to instances
	doneChan  chan *Invocation // instances to func
	instances *list.List

	// send chan to the kill chan to destroy the instance, then
	// wait for msg on sent chan to block until it is done
	killChan chan chan bool
}

// This is essentially a virtual sandbox.  It is backed by a real
// Sandbox (when it is allowed to allocate one).  It pauses/unpauses
// based on usage, and starts fresh instances when they die.
type LambdaInstance struct {
	lfunc *LambdaFunc

	// snapshot of LambdaFunc, at the time the LambdaInstance is created
	codeDir string
	meta    *sandbox.SandboxMeta

	// send chan to the kill chan to destroy the instance, then
	// wait for msg on sent chan to block until it is done
	killChan chan chan bool
}

// represents an HTTP request to be handled by a lambda instance
type Invocation struct {
	w http.ResponseWriter
	r *http.Request

	// signal to client that response has been written to w
	done chan bool

	// how many milliseconds did ServeHTTP take?  (doesn't count
	// queue time or Sandbox init)
	execMs int
}

func NewLambdaMgr() (res *LambdaMgr, err error) {
	mgr := &LambdaMgr{
		lfuncMap: make(map[string]*LambdaFunc),
		sbMap:    make(map[int]*SbMeta),
		cap:      0, // 初始为 0 代表没有容器存在, 第一个容器编号为 1
		lActive:  list.New(),
		lPause:   list.New(),
		//lRun:     list.New(),
		lEmpty:    list.New(),
		killWarmChan: make(chan bool, 1),
	}
	defer func() {
		if err != nil {
			log.Printf("Cleanup Lambda Manager due to error: %v", err)
			mgr.Cleanup()
		}
	}()

	// 在 open-lambda/test-dir/worker 下新建目录 code 并将路径赋值给 LambdaMgr.codeDirs
	mgr.codeDirs, err = common.NewDirMaker("code", common.Conf.Storage.Code.Mode()) // Mode: STORE_REGULAR
	if err != nil {
		return nil, err
	}

	// 显式保存 string 类型地共享代码目录路径 open-lambda/test-dir/worker/code
	// mgr.codeDirs 类型为 common.DirMaker
	mgr.codeDir = filepath.Join(common.Conf.Worker_dir, "code")

	// 在 open-lambda/test-dir/worker 下新建目录 scratch 并将路径赋值给 LambdaMgr.scratchDirs
	mgr.scratchDirs, err = common.NewDirMaker("scratch", common.Conf.Storage.Scratch.Mode()) // Mode: STORE_REGULAR
	if err != nil {
		return nil, err
	}

	log.Printf("Create SandboxPool")
	mgr.sbPool, err = sandbox.SandboxPoolFromConfig("sandboxes", common.Conf.Mem_pool_mb)
	if err != nil {
		return nil, err
	}

	log.Printf("Create DepTracer")
	mgr.DepTracer, err = NewDepTracer(filepath.Join(common.Conf.Worker_dir, "dep-trace.json"))
	if err != nil {
		return nil, err
	}

	log.Printf("Create PackagePuller")
	mgr.PackagePuller, err = NewPackagePuller(mgr, mgr.sbPool, mgr.DepTracer)
	if err != nil {
		return nil, err
	}

	if common.Conf.Features.Import_cache {
		log.Printf("Create ImportCache")
		mgr.ImportCache, err = NewImportCache(mgr.codeDirs, mgr.scratchDirs, mgr.sbPool, mgr.PackagePuller)
		if err != nil {
			return nil, err
		}
	}

	log.Printf("Create HandlerPuller")
	mgr.HandlerPuller, err = NewHandlerPuller(mgr.codeDirs)
	if err != nil {
		return nil, err
	}
	/*
	// 预启动一个容器并置于 Active 状态
	if err = mgr.Prewarm(2); err != nil {
		log.Printf("[lambda.go 166] prewarm sandbox failed during lambdamgr initiation\n")
		return nil, err
	}
	*/

	go mgr.PreWarmer()

	return mgr, nil
}

// 冷启动新的容器
func (mgr *LambdaMgr) LaunchSB() (sbMeta *SbMeta, err error)  {
	var sb sandbox.Sandbox
	var id int

	// 找到一个合适的 ID：1. 先查找 lEmpty；2. 如果 lEmpty 为空则从 cap 递增分配新 id
	if mgr.lEmpty.Len() > 0 {
		el := mgr.lEmpty.Front()
		id = el.Value.(int)

		mgr.lEmpty.Remove(el)
	} else {
		mgr.cap++
		id = mgr.cap
	}

	// 初始化容器需要绑定为 /host 的本地目录
	dirID := fmt.Sprintf("%d", id)
	scratchDir := filepath.Join(common.Conf.Worker_dir, "scratch", dirID)
	if _, err = os.Stat(scratchDir); os.IsNotExist(err) {
		if err = os.Mkdir(scratchDir, 0700); err != nil {
			return nil, err
		}
	}

	// 创建容器
	if sb, err = mgr.sbPool.Create(nil, true, mgr.codeDir, scratchDir, nil); err != nil {
		log.Printf("[lambda.go LaunchSB()] failed to create sb: %v\n", err)
		return nil, err
	}

	// 登记容器
	mgr.sbMapMutex.Lock()
	defer mgr.sbMapMutex.Unlock()

	sbMeta = &SbMeta{
		id:         id,
		sb:         sb,
		scratchDir: scratchDir,
	}
	mgr.sbMap[id] = sbMeta

	return sbMeta, nil
}

// TODO: 这里直接假定要新增 ID 分配给容器，然后创建容器
// 完整流程需要先判断 lFree 中是否有 ID 可复用；创建容器后需要更新 mgr 中的数据结构
func (mgr *LambdaMgr) Prewarm(size int) (err error) {
	log.Printf("[lambda.go 174] ready to prewarm %d sandboxes, lPause: %d\n", size, mgr.lPause.Len())
	var sbMeta *SbMeta = nil

	for i := 1; i <= size; i++ {
		if sbMeta, err = mgr.LaunchSB(); err != nil {
			log.Printf("[lambda.go Prewarm()] failed to launch a new sb: %v\n", err)
			return err
		}
		sb := sbMeta.sb
		
		if err = sb.Pause(); err != nil {
			log.Printf("[lambda.go Prewarm()]sandbox pause failed\n")
		}

		// TODO: 如何判断是加入 lActive 还是 lPause
		//mgr.lActive.PushBack(sbMeta.id)
		mgr.lPMutex.Lock()
		mgr.lPause.PushBack(sbMeta.id)
		mgr.lPMutex.Unlock()

		log.Printf("[lambda.go Prewarm()] sandbox[%d](%d/%d) launched and paused, lPause: %d\n", sbMeta.id, i, size, mgr.lPause.Len())
	}

	return nil
}

func (mgr *LambdaMgr) PreWarmer() {
	var lALen int = mgr.lActive.Len() // lActive 的原始长度/上一轮 for-select 循环结束时的长度
	var staticIncr int = 2 // 固定预留容器数量，即当 lActive = 0 且没有请求时，也要预留以备用的数量
	// var dynamicIncr int = 0
	var incr int = 0 // 实际新增的容器数量

	LOOP: for {
		select {
		case <- mgr.killWarmChan: // a signal to break out of for-select infinite loop	
			break LOOP // break 跳出 select 外层的 for 循环，如果不使用标签则只是跳出 select
		case <- time.After(time.Millisecond * time.Duration(5000)): // loop per 50ms to adjust the pool
			// 计算一个统计周期内可用容器数量的变化量，即：
			// 上一周期结束时 lActive 长度 + 上一周期结束后 PreWarmer 创建的容器 - 本周期开始时 lActive 长度
			// 若结果为正值则表示周期内可用容器数减少，说明需要在本轮补充容器
			// 若结果为负值则表示周期内可用容器数增加，可能来源于 linst 使用完释放的容器，则本轮不需补充
			deltaSB := lALen + incr - mgr.lActive.Len()
			lALen = mgr.lActive.Len()
			log.Printf("[lambda.go PreWarmer()] decreased %d available sandboxes; lActive: %d\n", deltaSB, mgr.lActive.Len())

			if deltaSB > 0 {
				incr = 1 // TODO: 如何确定需要增加的容器数
			} else {
				if mgr.lActive.Len() == 0 {
					incr = staticIncr
				} else {
					incr = 0 // TODO: 何时考虑释放容器资源
				}
			}
			
			// 如果要预热启动多个容器，则采用 goroutine 的方式并行执行容器创建
			// 但 PreWarmer 内采用 “获取指标->决定增量->实施创建” 的串行逻辑
			// 因此采用 WaitGroup 等待所有并行执行的 goroutine 结束后才进入下一轮 select 
			var wg = new(sync.WaitGroup)

			for i := 1; i <= incr; i++ {
				wg.Add(1)
				go func(i int, mgr *LambdaMgr, wg *sync.WaitGroup) {
					t1 := time.Now()
					log.Printf("[lambda.go PreWarmer()] goroutine[%d] began at %v ms; lActive: %d\n", 
									i, t1.UnixNano() / 1e6, mgr.lActive.Len())

					defer wg.Done()

					sbMeta, err := mgr.LaunchSB()
					if err != nil {
						log.Printf("[lambda.go PreWarmer()] goroutine[%d] failed to launch a new sb: %v\n", i, err)
						return
					}

					// TODO: 如何判断是加入 lActive 还是 lPause
					mgr.lAMutex.Lock()
					mgr.lActive.PushBack(sbMeta.id)
					mgr.lAMutex.Unlock()

					t2 := time.Now()
					log.Printf("[lamdba.go PreWarmer()] goroutine[%d] ended at %v ms; lActive: %d; launched sb[%d]; consumed %d ms\n", 
									i, t2.UnixNano() / 1e6, mgr.lActive.Len(), sbMeta.id, int64(t2.Sub(t1)) / 1000000)
				}(i, mgr, wg)
			}
			
			wg.Wait()
			log.Printf("[lambda.go PreWarmer()] launched %d new sandboxes; lActive: %d\n", incr, mgr.lActive.Len())
		}
	}

	log.Printf("[lambda.go Prewarmer()] PreWarmer terminated\n")
}

// 通过 killWarmChan 通知 PreWarmer 结束 for-select 循环
func (mgr *LambdaMgr) KillPreWarmer() {
	mgr.killWarmChan <- true
}

// 销毁 sbMap 中管理的所有容器
func (mgr *LambdaMgr) KillAllSb() {
	mgr.sbMapMutex.Lock()
	defer mgr.sbMapMutex.Unlock()

	for id, sbMeta := range mgr.sbMap {
		if sbMeta.sb != nil {
			sbMeta.sb.Destroy()
			log.Printf("[lambda.go KillAllSb()] sb[%d] been destroyed\n", id)
		}
	}
}

// Returns an existing instance (if there is one), or creates a new one
func (mgr *LambdaMgr) Get(name string) (f *LambdaFunc) {
	mgr.mapMutex.Lock()
	defer mgr.mapMutex.Unlock()

	f = mgr.lfuncMap[name]

	if f == nil {
		log.Printf("[lambda.go Get()] new entry lfuncMap[%s]\n", name)
		f = &LambdaFunc{
			lmgr:      mgr,
			name:      name,
			funcChan:  make(chan *Invocation, 32),
			instChan:  make(chan *Invocation, 32),
			doneChan:  make(chan *Invocation, 32),
			instances: list.New(),
			killChan:  make(chan chan bool, 1),
		}

		go f.Task()
		mgr.lfuncMap[name] = f
	}

	return f
}

func (mgr *LambdaMgr) Debug() string {
	return mgr.sbPool.DebugString() + "\n"
}

func (mgr *LambdaMgr) Cleanup() {
	mgr.KillPreWarmer()

	mgr.mapMutex.Lock() // don't unlock, because this shouldn't be used anymore

	// HandlerPuller+PackagePuller requires no cleanup

	// 1. cleanup handler Sandboxes
	// 2. cleanup Zygote Sandboxes (after the handlers, which depend on the Zygotes)
	// 3. cleanup SandboxPool underlying both of above
	for _, f := range mgr.lfuncMap {
		//log.Printf("[lambda.go Cleanup()] kill lfunc[%s]", f.name)
		f.Kill()
	}

	if mgr.ImportCache != nil {
		mgr.ImportCache.Cleanup()
	}

	if mgr.sbPool != nil {
		mgr.sbPool.Cleanup() // assumes all Sandboxes are gone
	}

	// cleanup DepTracer
	if mgr.DepTracer != nil {
		mgr.DepTracer.Cleanup()
	}

	if mgr.codeDirs != nil {
		mgr.codeDirs.Cleanup()
	}

	if mgr.scratchDirs != nil {
		mgr.scratchDirs.Cleanup()
	}

	mgr.KillAllSb() // 清理所有容器
}

func (f *LambdaFunc) Invoke(w http.ResponseWriter, r *http.Request) {
	t := common.T0("LambdaFunc.Invoke")
	defer t.T1()

	done := make(chan bool)
	req := &Invocation{w: w, r: r, done: done}

	// send invocation to lambda func task, if room in queue
	select {
	case f.funcChan <- req:
		// block until it's done
		<-done
	default:
		// queue cannot accept more, so reply with backoff
		req.w.WriteHeader(http.StatusTooManyRequests)
		req.w.Write([]byte("lambda function queue is full"))
	}
}

// add function name to each log message so we know which logs
// correspond to which LambdaFuncs
func (f *LambdaFunc) printf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	log.Printf("%s [FUNC %s]", strings.TrimRight(msg, "\n"), f.name)
}

// the function code may contain comments such as the following:
//
// # ol-install: parso,jedi,idna,chardet,certifi,requests
// # ol-import: parso,jedi,idna,chardet,certifi,requests,urllib3
//
// The first list should be installed with pip install.  The latter is
// a hint about what may be imported (useful for import cache).
//
// We support exact pkg versions (e.g., pkg==2.0.0), but not < or >.
// If different lambdas import different versions of the same package,
// we will install them, for example, to /packages/pkg==1.0.0/pkg and
// /packages/pkg==2.0.0/pkg.  We'll symlink the version the user wants
// to /handler/packages/pkg.  For example, two different lambdas might
// have links as follows:
//
// /handler/packages/pkg => /packages/pkg==1.0.0/pkg
// /handler/packages/pkg => /packages/pkg==2.0.0/pkg
//
// Lambdas should have /handler/packages in their path, but not
// /packages.
func parseMeta(codeDir string, funcName string) (meta *sandbox.SandboxMeta, err error) {
	installs := make([]string, 0)
	imports := make([]string, 0)

	//path := filepath.Join(codeDir, "f.py")
	path := filepath.Join(codeDir, funcName) + ".py"
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scnr := bufio.NewScanner(file)
	for scnr.Scan() { // Scan() 默认逐行读取，利用 for 进行循环遍历文件
		line := strings.ReplaceAll(scnr.Text(), " ", "") // Text() 将 Scanner 读取的内容生成为 string，然后将所有的 " " 替换为 "" 并返回新的副本
		parts := strings.Split(line, ":") // 以 ":" 为分隔符切割返回 []string 类型
		if parts[0] == "#ol-install" {
			for _, val := range strings.Split(parts[1], ",") { // for-range键值循环中 key 为切片下标 val 为下标对应的值，这里只需取切片中的值
				val = strings.TrimSpace(val) // 去掉字符串开头结尾的空格
				if len(val) > 0 {
					installs = append(installs, val)
				}
			}
		} else if parts[0] == "#ol-import" {
			for _, val := range strings.Split(parts[1], ",") {
				val = strings.TrimSpace(val)
				if len(val) > 0 {
					imports = append(imports, val)
				}
			}
		}
	}

	for i, pkg := range installs {
		installs[i] = normalizePkg(pkg)
	}

	return &sandbox.SandboxMeta{
		Installs: installs,
		Imports:  imports,
	}, nil
}

/*
// if there is any error:
// 1. we won't switch to the new code
// 2. we won't update pull time (so well check for a fix next tim)
func (f *LambdaFunc) pullHandlerIfStale() (err error) {
	// check if there is newer code, download it if necessary
	now := time.Now() // type Time 是以 ns 为精度计量时间，Now() 返回当前时间 
	cache_ns := int64(common.Conf.Registry_cache_ms) * 1000000 // Conf 中默认参数为 5s

	// should we check for new code?
	// 这里是设置了一个 interval = 5s 的间隔，如果这一次操作距离上一次 pull 的时间间隔 < interval
	// 则默认不执行本次操作，因此这里这个 interval 设置的时间需要斟酌，另外如作者注释所言：是否需要执行这样的检测
	if f.lastPull != nil && int64(now.Sub(*f.lastPull)) < cache_ns {
		log.Printf("[lambda.go 311] the time since the last handler/package PULL is less than cache_ns, skip pullHandleIfStale\n")
		return nil
	}

	log.Printf("[lambda.go 314] run pullHandlerIfStale, currently the LambdaFunc.codeDir is '%s'\n", f.codeDir)

	t := common.T0("PullHandler")

	// is there new code?
	// 建立目录 open-lambda/test-dir/worker/code/1001-echo
	// 将 open-lambda/test-registry/echo 下的文件 f.py 拷贝到上述目录中
	codeDir, err := f.lmgr.HandlerPuller.Pull(f.name)
	if err != nil {
		return err
	}

	t.T1()
	log.Printf("[lambda.go 321] handler pulled into '%s' using %d milliseconds\n", codeDir, t.Milliseconds)

	if codeDir == f.codeDir {
		return nil
	}

	defer func() {
		if err != nil {
			if err := os.RemoveAll(codeDir); err != nil {
				log.Printf("could not cleanup %s after failed pull", codeDir)
			}

			// we dirty this dir (e.g., by setting up
			// symlinks to packages, so we want the
			// HandlerPuller to give us a new one next
			// time, even if the code hasn't changed
			f.lmgr.HandlerPuller.Reset(f.name)
		}
	}()

	// inspect new code for dependencies; if we can install
	// everything necessary, start using new code
	meta, err := parseMeta(codeDir)
	if err != nil {
		return err
	}

	meta.Installs, err = f.lmgr.PackagePuller.InstallRecursive(meta.Installs)
	if err != nil {
		return err
	}
	f.lmgr.DepTracer.TraceFunction(codeDir, meta.Installs)

	f.codeDir = codeDir
	f.meta = meta
	f.lastPull = &now
	return nil
}
*/

// this Task receives lambda requests, fetches new lambda code as
// needed, and dispatches to a set of lambda instances.  Task also
// monitors outstanding requests, and scales the number of instances
// up or down as needed.
//
// communication for a given request is as follows (each of the four
// transfers are commented within the function):
//
// client -> function -> instance -> function -> client
//
// each of the 4 handoffs above is over a chan.  In order, those chans are:
// 1. LambdaFunc.funcChan
// 2. LambdaFunc.instChan
// 3. LambdaFunc.doneChan
// 4. Invocation.done
//
// If either LambdaFunc.funcChan or LambdaFunc.instChan is full, we
// respond to the client with a backoff message: StatusTooManyRequests
func (f *LambdaFunc) Task() {
	f.printf("debug: LambdaFunc[%s].Task() runs on goroutine %d", f.name, common.GetGoroutineID())

	// we want to perform various cleanup actions, such as killing
	// instances and deleting old code.  We want to do these
	// asyncronously, but in order.  Thus, we use a chan to get
	// FIFO behavior and a single cleanup task to get async.
	//
	// two types can be sent to this chan:
	//
	// 1. string: this is a path to be deleted
	//
	// 2. chan: this is a signal chan that corresponds to
	// previously initiated cleanup work.  We block until we
	// receive the complete signal, before proceeding to
	// subsequent cleanup tasks in the FIFO.
	cleanupChan := make(chan interface{}, 32)
	cleanupTaskDone := make(chan bool)
	go func() {
		for {
			msg, ok := <-cleanupChan
			if !ok {
				cleanupTaskDone <- true
				return
			}

			switch op := msg.(type) {
			case string:
				if err := os.RemoveAll(op); err != nil {
					f.printf("Async code cleanup could not delete %s, even after all instances using it killed: %v", op, err)
				}
			case chan bool:
				<-op
			}
		}
	}()

	// stats for autoscaling
	outstandingReqs := 0
	execMs := common.NewRollingAvg(10) // RollingAvg.size = 10
	var lastScaling *time.Time = nil
	timeout := time.NewTimer(0) // 最少 0 (ns) 后向 timeout->C 发送当前的时间

	for {
		select {
		case <-timeout.C:
			if f.codeDir == "" {
				continue
			}
		case req := <-f.funcChan:
			// msg: client -> function

			// check for new code, and cleanup old code
			// (and instances that use it) if necessary
		/*
		if comment == true {
			oldCodeDir := f.codeDir

			tHP := common.T0("HP") // Handler and Package pull 计时起点
			if err := f.pullHandlerIfStale(); err != nil {
				f.printf("[lambda.go 442] LambdaFunc[%s].Task() calling pullHandlerIfStale failed: %v", f.name, err)
				req.w.WriteHeader(http.StatusInternalServerError)
				req.w.Write([]byte(err.Error() + "\n"))
				req.done <- true
				continue
			}
			tHP.T1() // Handler and Package pull 计时终点
			log.Printf("[lambda.go 449] pull Handler and install Package consume %d milliseconds\n", tHP.Milliseconds)

			if oldCodeDir != "" && oldCodeDir != f.codeDir {
				el := f.instances.Front()
				for el != nil {
					log.Printf("[lambda.go 455] oldCodeDir '%s' != f.codeDir '%s', triger LambdaInstance.AsyncKill\n", oldCodeDir, f.codeDir)
					waitChan := el.Value.(*LambdaInstance).AsyncKill()
					cleanupChan <- waitChan
					el = el.Next()
				}
				f.instances = list.New()

				// cleanupChan is a FIFO, so this will
				// happen after the cleanup task waits
				// for all instance kills to finish
				cleanupChan <- oldCodeDir
			}

			f.lmgr.DepTracer.TraceInvocation(f.codeDir)
		}
		*/
			// 1. 判断目标 handler 是否在 mgr.codeDir
			// 2. 如果不存在则从 test-registry 拉取 .py 文件至 mgr.codeDir
			handlerPath := filepath.Join(f.lmgr.codeDir, f.name) + ".py" // 这里不能把 f.name 放在 filepath.Join 里，否则就成了 /f.name/.py
			if _, err := os.Stat(handlerPath); os.IsNotExist(err) {
				srcPath := filepath.Join(common.Conf.Registry, f.name) + ".py"
				log.Printf("[lambda.go lfunc.Task()] pull '%s.py', src: %s, dest: %s\n", f.name, srcPath, f.lmgr.codeDir)
				cmd := exec.Command("cp", srcPath, f.lmgr.codeDir)
				if err = cmd.Run(); err != nil {
					log.Printf("[lambda.go lfunc.Task()] failed to pull %s.py : %v\n", f.name, err)
					return
				}
			}

			// 1. 解析代码文件获取待下载的 pkg，这里修改了 parseMeta 的传参
			// 2. 调用 package.InstallRecursive 下载 pkg
			meta, err := parseMeta(f.lmgr.codeDir, f.name)
			if err != nil {
				log.Printf("[lambda.go lfunc.Task()] parseMeta failed with err: %v\n", err)
				return
			}

			if meta.Installs, err = f.lmgr.PackagePuller.InstallRecursive(meta.Installs); err != nil {
				log.Printf("[lambda.go lfunc.Task()] download pkgs failed with err: %v\n", err)
				return
			}
			f.meta = meta

			select {
			case f.instChan <- req:
				// msg: function -> instance
				outstandingReqs += 1
			default:
				// queue cannot accept more, so reply with backoff
				req.w.WriteHeader(http.StatusTooManyRequests)
				req.w.Write([]byte("lambda instance queue is full"))
				req.done <- true
			}
		case req := <-f.doneChan:
			log.Printf("[lambda.go lfunc.Task()] f.doneChan recv signal\n")
			// msg: instance -> function

			execMs.Add(req.execMs)
			outstandingReqs -= 1

			// msg: function -> client
			req.done <- true

		case done := <-f.killChan:
			log.Printf("[lambda.go lfunc.Task()] f.killChan recv signal\n")
			// signal all instances to die, then wait for
			// cleanup task to finish and exit
			el := f.instances.Front()
			for el != nil {
				waitChan := el.Value.(*LambdaInstance).AsyncKill()
				cleanupChan <- waitChan
				el = el.Next()
			}
			if f.codeDir != "" {
				//cleanupChan <- f.codeDir
			}
			close(cleanupChan)
			<-cleanupTaskDone
			done <- true
			return
		}

		// POLICY: how many instances (i.e., virtual sandboxes) should we allocate?

		// AUTOSCALING STEP 1: decide how many instances we want

		// let's aim to have 1 sandbox per second of outstanding work
		inProgressWorkMs := outstandingReqs * execMs.Avg
		desiredInstances := inProgressWorkMs / 1000

		//log.Printf("[lambda.go lfunc.Task()] outstandingReqs = %d, desiredInstances = %d\n", outstandingReqs, desiredInstances)

		// if we have, say, one job that will take 100
		// seconds, spinning up 100 instances won't do any
		// good, so cap by number of outstanding reqs
		if outstandingReqs < desiredInstances {
			desiredInstances = outstandingReqs
		}

		// always try to have one instance
		if desiredInstances < 1 {
			desiredInstances = 1
		}

		// AUTOSCALING STEP 2: tweak how many instances we have, to get closer to our goal

		// make at most one scaling adjustment per second
		//adjustFreq := time.Second // 1s
		adjustFreq := time.Millisecond // 1ms
		now := time.Now()
		if lastScaling != nil {
			elapsed := now.Sub(*lastScaling)
			if elapsed < adjustFreq {
				//log.Printf("[lambda.go lfunc.Task()] elapsed time < 1s\n")
				if desiredInstances != f.instances.Len() {
					//log.Printf("[lambda.go lfunc.Task()] adjust new timeout interval\n")
					timeout = time.NewTimer(adjustFreq - elapsed)
				}
				continue
			}
		}

		// kill or start at most one instance to get closer to
		// desired number
		if f.instances.Len() < desiredInstances {
			f.printf("increase instances to %d", f.instances.Len()+1)
			f.newInstance()
			lastScaling = &now
		} else if f.instances.Len() > desiredInstances {
			//log.Printf("[lambda.go lfunc.Task()] execute LambdaInstance AsyncKill\n")
			f.printf("reduce instances to %d", f.instances.Len()-1)
			waitChan := f.instances.Back().Value.(*LambdaInstance).AsyncKill()
			f.instances.Remove(f.instances.Back())
			cleanupChan <- waitChan
			lastScaling = &now
		}

		if f.instances.Len() != desiredInstances {
			//log.Printf("[lambda.go lfunc.Task()] current instances numbers[%d] != desired numbers[%d]\n", f.instances.Len(), desiredInstances)
			// we can only adjust quickly, so we want to
			// run through this loop again as soon as
			// possible, even if there are no requests to
			// service.
			timeout = time.NewTimer(adjustFreq)
		}
	}
}

func (f *LambdaFunc) newInstance() {
	/*
	if f.codeDir == "" {
		panic("cannot start instance until code has been fetched")
	}
	*/

	linst := &LambdaInstance{
		lfunc:    f,
		codeDir:  f.codeDir,
		meta:     f.meta,
		killChan: make(chan chan bool, 1),
	}

	f.instances.PushBack(linst)

	go linst.Task()
}

func (f *LambdaFunc) Kill() {
	done := make(chan bool)
	f.killChan <- done
	<-done
}

// 获取一个可用的 sandbox
// 1. 从 lActive 中寻找
// 2. 当 lActive 为空时，从 lPause 中寻找
// 3. 当 lActive 和 lPause 中均为空时冷启动
func (mgr *LambdaMgr) GetSB(name string) (sbMeta *SbMeta, err error) {
	// 搜索 lActive
	if mgr.lActive.Len() > 0 {
		mgr.lAMutex.Lock()

		el := mgr.lActive.Front()
		id := el.Value.(int)

		mgr.lActive.Remove(el)
		//mgr.lRun.PushBack(id)

		sbMeta = mgr.sbMap[id]

		mgr.lAMutex.Unlock()

		log.Printf("[lambda.go GetSB()] %s get sb[%d] from lActive\n", name, id)
	} else if mgr.lPause.Len() > 0 {
		mgr.lPMutex.Lock()

		el := mgr.lPause.Front()
		id := el.Value.(int)

		mgr.lPause.Remove(el)
		//mgr.lRun.PushBack(id)

		sbMeta = mgr.sbMap[id]
		if err = sbMeta.sb.Unpause(); err != nil {
			log.Printf("[lambda.go GetSB()] Unpause sb failed: %v\n", err)
			return nil, err
		}

		mgr.lPMutex.Unlock()

		log.Printf("[lambda.go GetSB()] %s get sb[%d] from lPause\n", name, id)
	} else {
		if sbMeta, err = mgr.LaunchSB(); err != nil {
			log.Printf("[lambda.go GetSB()] failed to cold start a new sb: %v", err)
			return nil, err
		}

		log.Printf("[lambda.go GetSB()] %s get new sb[%d] by LaunchSB()\n", name, sbMeta.id)
	}

	return sbMeta, nil
}

func (mgr *LambdaMgr) RecycleSB(sbMeta *SbMeta) {
	// TODO: 如何判断是加入 lActive 还是 lPause

	if err := sbMeta.sb.Pause(); err != nil {
		log.Printf("[lambda.go RecycleSB()] failed to pause for: %v\n", err)
		// TODO: return err? or do something
	}

	mgr.lPMutex.Lock()
	mgr.lPause.PushBack(sbMeta.id)
	mgr.lPMutex.Unlock()

	log.Printf("[lambda.go RecycleSB()] paused sb[%d] and add to lPause: %d\n", sbMeta.id, mgr.lPause.Len())
}

// this Task manages a single Sandbox (at any given time), and
// forwards requests from the function queue to that Sandbox.
// when there are no requests, the Sandbox is paused.
//
// These errors are handled as follows by Task:
//
// 1. Sandbox.Pause/Unpause: discard Sandbox, create new one to handle request
// 2. Sandbox.Create/Channel: discard Sandbox, propagate HTTP 500 to client
// 3. Error inside Sandbox: simply propagate whatever occured to client (TODO: restart Sandbox)
func (linst *LambdaInstance) Task() {
	f := linst.lfunc

	var sb sandbox.Sandbox = nil
	//var client *http.Client = nil // whenever we create a Sandbox, we init this too
	var proxy *httputil.ReverseProxy = nil // whenever we create a Sandbox, we init this too

	for {
		// wait for a request (blocking) before making the
		// Sandbox ready, or kill if we receive that signal
		var req *Invocation
		select {
		case req = <-f.instChan:
		case killed := <-linst.killChan:
			log.Printf("[lambda.go linst.Task()] linst.killChan recv signal\n")
			if sb != nil {
				sb.Destroy()
			}
			killed <- true
			return
		}
	/*
	if comment == true {
		// if we have a sandbox, try unpausing it to see if it is still alive
		if sb != nil {
			// Unpause will often fail, because evictors
			// are likely to prefer to evict paused
			// sandboxes rather than inactive sandboxes.
			// Thus, if this fails, we'll try to handle it
			// by just creating a new sandbox.
			log.Printf("[lambda.go 636] ready to unpause an existed paused sandbox\n")
			tUNPAUSE := common.T0("sbUnpause") // Unpause an existed paused sandbox 计时起点
			if err := sb.Unpause(); err != nil {
				f.printf("discard sandbox %s due to Unpause error: %v", sb.ID(), err)
				sb = nil
			}
			tUNPAUSE.T1() // Unpause an existed paused sandbox 计时终点
			log.Printf("[lambda.go 665] sandbox launch through unpause consume %d milliseconds\n", tUNPAUSE.Milliseconds)
		}

		// if we don't already have a Sandbox, create one, and
		// HTTP proxy over the channel
		if sb == nil {
			sb = nil
			if f.lmgr.ImportCache != nil {
				scratchDir := f.lmgr.scratchDirs.Make(f.name)

				// we don't specify parent SB, because ImportCache.Create chooses it for us
				sb, err = f.lmgr.ImportCache.Create(f.lmgr.sbPool, true, linst.codeDir, scratchDir, linst.meta)
				if err != nil {
					f.printf("failed to get Sandbox from import cache")
					sb = nil
				}
			}

			// import cache is either disabled or it failed
			if sb == nil {
				scratchDir := f.lmgr.scratchDirs.Make(f.name)
				log.Printf("[lambda.go 658] ready to create a new sandbox while import cache is disabled\n")
				log.Printf("[lambda.go 684] codeDir is '%s', scratchDir is '%s'\n", linst.codeDir, scratchDir)
				tCREATE := common.T0("sbCreate") // Create a new sandbox 计时起点
				sb, err = f.lmgr.sbPool.Create(nil, true, linst.codeDir, scratchDir, linst.meta)
				//sb, err = f.lmgr.sbPool.Create(nil, true, linst.codeDir, scratchDir, nil) // 这里传入的 meta 实际暂时看没什么用
				tCREATE.T1() // Create a new sandbox 计时终点
				log.Printf("[lambda.go 665] sandbox launch through create consume %d milliseconds\n", tCREATE.Milliseconds)
			}

			if err != nil {
				req.w.WriteHeader(http.StatusInternalServerError)
				req.w.Write([]byte("could not create Sandbox: " + err.Error() + "\n"))
				f.doneChan <- req
				continue // wait for another request before retrying
			}
			
			proxy, err = sb.HttpProxy()
			if err != nil {
				req.w.WriteHeader(http.StatusInternalServerError)
				req.w.Write([]byte("could not connect to Sandbox: " + err.Error() + "\n"))
				f.doneChan <- req
				f.printf("discard sandbox %s due to Channel error: %v", sb.ID(), err)
				sb = nil
				continue // wait for another request before retrying
			}
			
		}
	}
	*/

		sbMeta, err := f.lmgr.GetSB("lfunc-" + f.name)
		if err != nil {
			log.Printf("[lambda.go linst.Task()] failed to get a sb: %v\n", err)
			return
		}

		sb = sbMeta.sb
		//log.Printf("[lambda.go linst.Task()] lfunc[%s] instance get sb[%d]\n", f.name, sbMeta.id)

		proxy, err = sb.HttpProxy()
		if err != nil {
			req.w.WriteHeader(http.StatusInternalServerError)
			req.w.Write([]byte("could not connect to Sandbox: " + err.Error() + "\n"))
			f.doneChan <- req
			f.printf("discard sandbox %s due to Channel error: %v", sb.ID(), err)
			sb = nil
			continue // wait for another request before retrying
		}

		pipeFile := filepath.Join(sbMeta.scratchDir, "server_pipe")
		pipe, err := os.OpenFile(pipeFile, os.O_RDWR, 0777) // 以读写模式打开命名管道文件
		if err != nil {
			log.Printf("[lambda.go linst.Task()] cannot open server_pipe: %v\n", err)
			return
		}
		defer pipe.Close()
		
		fname := []byte(f.name) // LambdaFunc.name 保存了请求名(/run/请求名)
		if _, err = pipe.Write(fname); err != nil {
			log.Printf("[lambda.go linst.Task()] failed to write filename[%s] to server_pipe\n", f.name)
		}
		//log.Printf("[lambda.go linst.Task()] filename[%s] has been written to server_pipe\n", f.name)

		// below here, we're guaranteed (1) sb != nil, (2) proxy != nil, (3) sb is unpaused

		// serve until we incoming queue is empty
		for req != nil {
			//log.Printf("[lambda.go linst.Task()] begin to serve HTTP request\n")
			// ask Sandbox to respond, via HTTP proxy
			tHTTP := common.T0("ServeHTTP")
			proxy.ServeHTTP(req.w, req.r)
			tHTTP.T1()
			//log.Printf("[lambda.go linst.Task()] ServeHTTP consume %d milliseconds\n", tHTTP.Milliseconds)
			req.execMs = int(tHTTP.Milliseconds)
			f.doneChan <- req

			// check whether we should shutdown (non-blocking)
			select {
			case killed := <-linst.killChan:
				log.Printf("[lambda.go linst.Task()] linst.killChan recv signal\n")
				sb.Destroy() // TODO: 是否需要判断 sb != nil
				killed <- true
				return
			default:
			}

			// grab another request (non-blocking)
			select {
			case req = <-f.instChan:
			default:
				req = nil
			}
		}

		f.lmgr.RecycleSB(sbMeta)
		sb = nil // 将 sb 从 linst 中释放
	}
}

// signal the instance to die, return chan that can be used to block
// until it's done
func (linst *LambdaInstance) AsyncKill() chan bool {
	done := make(chan bool)
	linst.killChan <- done
	return done
}