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

	// thread-safe map from a lambda's name to its LambdaFunc
	mapMutex sync.Mutex
	lfuncMap map[string]*LambdaFunc

	// 用于维护 docker-pool 的数据结构
	sbMap   map[int]*SbMeta // map 记录了从 “容器ID” 到 “容器状态结构体” 的映射
	cap     int // docker-pool 的容量，也即当前最大容器编号，若新增容器则从该编号递增
	lActive *list.List // 处于 active&空闲 状态的容器的ID列表
	lPause  *list.List // 处于 paused&空闲 状态的容器的ID列表
	//lRun    *list.List // 处于 正在执行任务 状态的容器的ID列表
	lEmpty   *list.List // 当容器被销毁后空出来的ID列表，复用ID时从该列表选取编号

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
	mgr.PackagePuller, err = NewPackagePuller(mgr.sbPool, mgr.DepTracer)
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

	// 预启动一个容器并置于 Active 状态
	if err = mgr.Prewarm(1); err != nil {
		log.Printf("[lambda.go 166] prewarm sandbox failed during lambdamgr initiation\n")
		return nil, err
	}

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
	codeDir := filepath.Join(common.Conf.Worker_dir, "code")

	// 创建容器
	if sb, err = mgr.sbPool.Create(nil, true, codeDir, scratchDir, nil); err != nil {
		log.Printf("[lambda.go LaunchSB()] failed to create sb: %v\n", err)
		return nil, err
	}

	// 登记容器
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
	log.Printf("[lambda.go 174] begin to prewarm %d sandboxes\n", size)
	var sbMeta *SbMeta = nil

	for i := 1; i <= size; i++ {
		if sbMeta, err = mgr.LaunchSB(); err != nil {
			log.Printf("[lambda.go Prewarm()] failed to launch a new sb: %v\n", err)
			return err
		}
		sb := sbMeta.sb
		log.Printf("[lambda.go 201] successfully prewarmed sandbox[id=%d] of %d/%d\n", sbMeta.id, i, size)

		// 拉取代码至容器绑定的代码目录
		srcPath := filepath.Join(common.Conf.Registry, "echo.py")
		codeDir := filepath.Join(common.Conf.Worker_dir, "code")
		log.Printf("[lambda.go 195] srcPath: '%s', codeDir: '%s'\n", srcPath, codeDir)
		cmd := exec.Command("cp", "-r", srcPath, codeDir)
		if err = cmd.Run(); err != nil {
			log.Printf("[lambda.go 210] cp f.py failed with err: %v\n", err)
			return err
		}

		if err = sb.Pause(); err != nil {
			log.Printf("sandbox pause failed\n")
		}

		log.Printf("sandbox paused\n")

		log.Printf("step0: lPause len = %d\n", mgr.lPause.Len())
		mgr.lPause.PushBack(sbMeta.id)
		log.Printf("step1: lPause len = %d\n", mgr.lPause.Len())
	}

	return nil
}

// Returns an existing instance (if there is one), or creates a new one
func (mgr *LambdaMgr) Get(name string) (f *LambdaFunc) {
	mgr.mapMutex.Lock()
	defer mgr.mapMutex.Unlock()

	f = mgr.lfuncMap[name]

	if f == nil {
		log.Printf("[lambda.go 151] lfuncMap[%s] is nil, new LambdaFunc", name)
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
	mgr.mapMutex.Lock() // don't unlock, because this shouldn't be used anymore

	// HandlerPuller+PackagePuller requires no cleanup

	// 1. cleanup handler Sandboxes
	// 2. cleanup Zygote Sandboxes (after the handlers, which depend on the Zygotes)
	// 3. cleanup SandboxPool underlying both of above
	for _, f := range mgr.lfuncMap {
		log.Printf("Kill function: %s", f.name)
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
}

func (f *LambdaFunc) Invoke(w http.ResponseWriter, r *http.Request) {
	log.Printf("[lambda.go 209] execute Invoke()")
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
func parseMeta(codeDir string) (meta *sandbox.SandboxMeta, err error) {
	installs := make([]string, 0)
	imports := make([]string, 0)

	path := filepath.Join(codeDir, "f.py")
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

	loopcnt := 0
	for {
		loopcnt += 1
		log.Printf("[lambda.go 426] LambdaFunc[%s].Task() for loops the %d times\n", f.name, loopcnt)
		select {
		case <-timeout.C:
			log.Printf("[lambda.go 429] LambdaFunc[%s] received timeout signal\n", f.name)
			if f.codeDir == "" {
				continue
			}
		case req := <-f.funcChan:
			// msg: client -> function

			// check for new code, and cleanup old code
			// (and instances that use it) if necessary
			/*oldCodeDir := f.codeDir

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
			*/

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
			log.Printf("[lambda.go 462] LambdaFunc received doneChan signal\n")
			// msg: instance -> function

			execMs.Add(req.execMs)
			outstandingReqs -= 1

			// msg: function -> client
			req.done <- true

		case done := <-f.killChan:
			log.Printf("[lambda.go 472] LambdaFunc received killChan signal\n")
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

		log.Printf("[lambda.go 507] LambdaFunc.Task() executing after select\n")

		// POLICY: how many instances (i.e., virtual sandboxes) should we allocate?

		// AUTOSCALING STEP 1: decide how many instances we want

		// let's aim to have 1 sandbox per second of outstanding work
		inProgressWorkMs := outstandingReqs * execMs.Avg
		desiredInstances := inProgressWorkMs / 1000

		log.Printf("[lambda.go 543] outstandingReqs = %d, desiredInstances = %d\n", outstandingReqs, desiredInstances)

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
				log.Printf("[lambda.go 538] elapsed time < 1s\n")
				if desiredInstances != f.instances.Len() {
					log.Printf("[lambda.go 540] LambdaFunc adjust new timeout interval\n")
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
			log.Printf("[lambda.go 554] execute LambdaInstance AsyncKill\n")
			f.printf("reduce instances to %d", f.instances.Len()-1)
			waitChan := f.instances.Back().Value.(*LambdaInstance).AsyncKill()
			f.instances.Remove(f.instances.Back())
			cleanupChan <- waitChan
			lastScaling = &now
		}

		if f.instances.Len() != desiredInstances {
			log.Printf("[lambda.go 563] current instances numbers != desired numbers\n")
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
func (linst *LambdaInstance) GetSB() (sbMeta *SbMeta, err error) {
	mgr := linst.lfunc.lmgr

	// 搜索 lActive
	if mgr.lActive.Len() > 0 {
		el := mgr.lActive.Front()
		id := el.Value.(int)

		mgr.lActive.Remove(el)
		//mgr.lRun.PushBack(id)

		sbMeta = mgr.sbMap[id]
	} else if mgr.lPause.Len() > 0 {
		el := mgr.lPause.Front()
		id := el.Value.(int)

		mgr.lPause.Remove(el)
		//mgr.lRun.PushBack(id)

		sbMeta = mgr.sbMap[id]
		if err = sbMeta.sb.Unpause(); err != nil {
			log.Printf("[lambda.go GetSB()] Unpause sb failed: %v\n", err)
			return nil, err
		}
	} else {
		log.Printf("[lambda.go GetSB()] no sb available in lActive and lPause\n")

		if sbMeta, err = mgr.LaunchSB(); err != nil {
			log.Printf("[lambda.go GetSB()] failed to cold start a new sb: %v", err)
			return nil, err
		}
	}

	return sbMeta, nil
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

	var sbMeta *SbMeta = nil
	var sb sandbox.Sandbox = nil
	//var client *http.Client = nil // whenever we create a Sandbox, we init this too
	var proxy *httputil.ReverseProxy = nil // whenever we create a Sandbox, we init this too
	var err error

	for {
		// wait for a request (blocking) before making the
		// Sandbox ready, or kill if we receive that signal
		var req *Invocation
		select {
		case req = <-f.instChan:
		case killed := <-linst.killChan:
			log.Printf("[lambda.go 620] LambdaInstance killChan actived and destroy sb\n")
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

		if sbMeta, err = linst.GetSB(); err != nil {
			log.Printf("[lambda.go LambdaInstance.Task()] failed to get a sb: %v\n", err)
			return
		}
		sb = sbMeta.sb

		proxy, err = sb.HttpProxy()
		if err != nil {
			req.w.WriteHeader(http.StatusInternalServerError)
			req.w.Write([]byte("could not connect to Sandbox: " + err.Error() + "\n"))
			f.doneChan <- req
			f.printf("discard sandbox %s due to Channel error: %v", sb.ID(), err)
			sb = nil
			continue // wait for another request before retrying
		}

		log.Printf("[lambda.go 811] begin to write py filename to server_pipe\n")
		// TODO: pipeFile 路径的前面部分实际就是容器创建时的 scratchDir，因此是否需考虑将该路径保存在容器对应的 sbStats 中
		pipeFile := filepath.Join(sbMeta.scratchDir, "server_pipe")
		pipe, err := os.OpenFile(pipeFile, os.O_RDWR, 0777) // 以读写模式打开命名管道文件
		if err != nil {
			log.Printf("[lambda.go 815] cannot open server_pipe: %v\n", err)
			return
		}
		defer pipe.Close()
		
		bytes := []byte(f.name) // LambdaFunc.name 保存了请求名(/run/请求名)
		if _, err = pipe.Write(bytes); err != nil {
			log.Printf("[lambda.go 823] failed to write func name to server_pipe\n")
		}

		// below here, we're guaranteed (1) sb != nil, (2) proxy != nil, (3) sb is unpaused

		// serve until we incoming queue is empty
		for req != nil {
			log.Printf("[lambda.go 685] begin to serve HTTP request\n")
			// ask Sandbox to respond, via HTTP proxy
			tHTTP := common.T0("ServeHTTP")
			proxy.ServeHTTP(req.w, req.r)
			tHTTP.T1()
			log.Printf("[lambda.go 690] ServeHTTP consume %d milliseconds\n", tHTTP.Milliseconds)
			req.execMs = int(tHTTP.Milliseconds)
			f.doneChan <- req

			// check whether we should shutdown (non-blocking)
			select {
			case killed := <-linst.killChan:
				log.Printf("[lambda.go 697] execute sb destroy in LambdaInstance.Task()\n")
				sb.Destroy()
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

		if err := sb.Pause(); err != nil {
			f.printf("discard sandbox %s due to Pause error: %v", sb.ID(), err)
			sb = nil
		}
		log.Printf("[lambda.go 720] sandbox is paused\n")

		//f.lmgr.lRun.Remove(f.lmgr.lRun.Back())
		f.lmgr.lPause.PushBack(sbMeta.id)
		log.Printf("step4: lPause len = %d\n", f.lmgr.lPause.Len())
	}
}

// signal the instance to die, return chan that can be used to block
// until it's done
func (linst *LambdaInstance) AsyncKill() chan bool {
	done := make(chan bool)
	linst.killChan <- done
	return done
}