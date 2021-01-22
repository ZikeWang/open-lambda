package lambda

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/open-lambda/open-lambda/ol/common"
	"github.com/open-lambda/open-lambda/ol/sandbox"
)

// we invoke this lambda to do the pip install in a Sandbox.
//
// the install is not recursive (it does not install deps), but it
// does parse and return a list of deps, based on a rough
// approximation of the PEP 508 format.  We ignore the "extra" marker
// and version numbers (assuming the latest).
const installLambda = `
#!/usr/bin/env python
import os, sys, platform, re

def format_full_version(info):
    version = '{0.major}.{0.minor}.{0.micro}'.format(info)
    kind = info.releaselevel
    if kind != 'final':
        version += kind[0] + str(info.serial)
    return version

# as specified here: https://www.python.org/dev/peps/pep-0508/#environment-markers
os_name = os.name
sys_platform = sys.platform
platform_machine = platform.machine()
platform_python_implementation = platform.python_implementation()
platform_release = platform.release()
platform_system = platform.system()
platform_version = platform.version()
python_version = platform.python_version()[:3]
python_full_version = platform.python_version()
implementation_name = sys.implementation.name
if hasattr(sys, 'implementation'):
    implementation_version = format_full_version(sys.implementation.version)
else:
    implementation_version = "0"
extra = '' # TODO: support extras

def matches(markers):
    return eval(markers)

def top(dirname):
    path = None
    for name in os.listdir(dirname):
        if name.endswith('-info'):
            path = os.path.join(dirname, name, "top_level.txt")
    if path == None or not os.path.exists(path):
        return []
    with open(path) as f:
        return f.read().strip().split("\n")

def deps(dirname):
    path = None
    for name in os.listdir(dirname):
        if name.endswith('-info'):
            path = os.path.join(dirname, name, "METADATA")
    if path == None or not os.path.exists(path):
        return []

    rv = set()
    with open(path, encoding='utf-8') as f:
        for line in f:
            prefix = 'Requires-Dist: '
            if line.startswith(prefix):
                line = line[len(prefix):].strip()
                parts = line.split(';')
                if len(parts) > 1:
                    match = matches(parts[1])
                else:
                    match = True
                if match:
                    name = re.split(' \(', parts[0])[0]
                    rv.add(name)
    return list(rv)

def f(event):
    pkg = event["pkg"]
    alreadyInstalled = event["alreadyInstalled"]
    path = "/packages/" + pkg + "/files"
    if not alreadyInstalled:
        rc = os.system('pip3 install --no-deps %s -t %s' % (pkg, path))
        print('pip install returned code %d' % rc)
        assert(rc == 0)
    name = pkg.split("==")[0]
    d = deps(path)
    t = top(path)
    return {"Deps":d, "TopLevel":t}
`

/*
 * PackagePuller is the interface for installing pip packages locally.
 * The manager installs to the worker host from an optional pip mirror.
 */
type PackagePuller struct {
	lmgr      *LambdaMgr
	sbPool    sandbox.SandboxPool
	depTracer *DepTracer

	// directory of lambda code that installs pip packages
	//pipLambda string

	packages  sync.Map

	//sb        sandbox.Sandbox // 容器实际对象
}

type Package struct {
	name         string
	meta         PackageMeta
	installMutex sync.Mutex
	installed    uint32
}

// the pip-install admin lambda returns this
type PackageMeta struct {
	Deps     []string `json:"Deps"`
	TopLevel []string `json:"TopLevel"`
}
// 创建 open-lambda/test-dir/worker/admin-lambda/pip-install/f.py 并写入上面 installLambda 的内容
func NewPackagePuller(lmgr *LambdaMgr, sbPool sandbox.SandboxPool, depTracer *DepTracer) (*PackagePuller, error) {
	// create a lambda function for installing pip packages.  We do
	// each install in a Sandbox for two reasons:
	//
	// 1. packages may be malicious
	// 2. we want to install the right version, matching the Python
	//    in the Sandbox
	/*
	pipLambda := filepath.Join(common.Conf.Worker_dir, "admin-lambdas", "pip-install") // 创建目录 open-lambda/test-dir/worker/admin-lambdas/pip-install
	if err := os.MkdirAll(pipLambda, 0700); err != nil {
		return nil, err
	}
	
	path := filepath.Join(pipLambda, "installLambda.py") // 创建文件 open-lambda/test-dir/worker/admin-lambdas/pip-install/f.py
	*/
	path := filepath.Join(lmgr.codeDir, "installLambda.py")
	code := []byte(installLambda)
	if err := ioutil.WriteFile(path, code, 0600); err != nil {
		return nil, err
	}

	/*
	// 创建容器
	meta := &sandbox.SandboxMeta{
		MemLimitMB: common.Conf.Limits.Installer_mem_mb,
	}
	// pipLambda路径： open-lambda/test-dir/worker/admin-lambdas/pip-install
	// common.Conf.Pkgs_dir路径：open-lambda/test-dir/lambda/packages
	sb, err := sbPool.Create(nil, true, pipLambda, common.Conf.Pkgs_dir, meta)
	if err != nil {
		log.Printf("[packagePuller.go NewPackagePuller()] Failed to create sandbox with err: %s\n", err)
		return nil, err
	}
	*/

	installer := &PackagePuller{
		lmgr:      lmgr,
		sbPool:    sbPool,
		depTracer: depTracer,
		//pipLambda: pipLambda,
		//sb:        sb,
	}

	return installer, nil
}

// From PEP-426: "All comparisons of distribution names MUST
// be case insensitive, and MUST consider hyphens and
// underscores to be equivalent."
// 字符串小写化，并将 '_' 替换为 '-'
func normalizePkg(pkg string) string {
	return strings.ReplaceAll(strings.ToLower(pkg), "_", "-")
}

// "pip install" missing packages to Conf.Pkgs_dir
func (pp *PackagePuller) InstallRecursive(installs []string) ([]string, error) {
	// shrink capacity to length so that our appends are not
	// visible to caller
	/*
	s = s[low : high : max] 切片的三个参数的切片截取的意义为:
	low 为截取的起始下标(包含)，high 为截取的结束下标(不含)，max 为切片保留的原切片的最大下标(不含)
	即新切片从老切片的 low 下标元素开始，len = high - low, cap = max - low
	high 和 max 一旦超出在老切片中越界，就会发生 runtime err，slice out of range
	如果省略第三个参数的时候，第三个参数默认和第二个参数相同，即 len = cap
	*/
	installs = installs[:len(installs):len(installs)] // 限定了新切片最大的容量为原大小

	installSet := make(map[string]bool)
	for _, install := range installs {
		name := strings.Split(install, "==")[0] // name 取第一个 == 前的字符串
		installSet[name] = true
	}

	// Installs may grow as we loop, because some installs have
	// deps, leading to other installs
	for i := 0; i < len(installs); i++ {
		pkg := installs[i]
		if common.Conf.Trace.Package {
			log.Printf("[packagePuller.go InstallRecursive()] On '%v' of '%v'", pkg, installs)
		}
		p, err := pp.GetPkg(pkg)
		if err != nil {
			return nil, err
		}
		/*
		if common.Conf.Trace.Package {
			log.Printf("Package '%s' has deps %v", pkg, p.meta.Deps)
			log.Printf("Package '%s' has top-level modules %v", pkg, p.meta.TopLevel)
		}
		*/

		log.Printf("[packagePuller.go InstallRecursive()] Pkg[%s] has Deps: %v; TopLevel: %v\n", pkg, p.meta.Deps, p.meta.TopLevel)

		// push any previously unseen deps on the list of ones to install
		for _, dep := range p.meta.Deps {
			if !installSet[dep] {
				installs = append(installs, dep)
				installSet[dep] = true
			}
		}
	}

	return installs, nil
}

// does the pip install in a Sandbox, taking care to never install the
// same Sandbox more than once.
//
// the fast/slow path code is tweaked from the sync.Once code, the
// difference being that may try the installed more than once, but we
// will never try more after the first success
func (pp *PackagePuller) GetPkg(pkg string) (*Package, error) {
	// get (or create) package
	pkg = normalizePkg(pkg)
	tmp, _ := pp.packages.LoadOrStore(pkg, &Package{name: pkg}) // 对于这样一个 string->&Package{} 的映射，如果在 pp.packages 中有则直接返回 val，如果没有则先插入 <key, val> 映射再返回 val
	p := tmp.(*Package) // 返回值 tmp 是 interface{}类型，其实际是 Package{} 这个 struct 的指针，因此要做类型转换访问，*Package 表示访问该结构体指针指向的结构体对象，tmp.(Type) 表示将 interface{} 类型转换为要得到的实际类型

	// fast path
	if atomic.LoadUint32(&p.installed) == 1 { // 原子访问，从 &p.installed 的内存地址处读值，判断该值是否等于 1，即是否已经 installed
		log.Printf("[packagePuller.go GetPkg()] pkg[%s] already installed\n", p.name)
		return p, nil
	}

	// slow path
	log.Printf("[packagePuller.go GetPkg()] pkg[%s] ready to install\n", p.name)
	p.installMutex.Lock()
	defer p.installMutex.Unlock()
	if p.installed == 0 {
		if err := pp.sandboxInstall(p); err != nil {
			return p, err
		} else {
			atomic.StoreUint32(&p.installed, 1)
			pp.depTracer.TracePackage(p)
			return p, nil
		}
	}

	return p, nil
}

// do the pip install within a new Sandbox, to a directory mapped from
// the host.  We want the package on the host to share with all, but
// want to run the install in the Sandbox because we don't trust it.
func (pp *PackagePuller) sandboxInstall(p *Package) (err error) {
	// the pip-install lambda installs to /host, which is the the
	// same as scratchDir, which is the same as a sub-directory
	// named after the package in the packages dir

	// scratchDir路径：open-lambda/test-dir/lambda/packages/<package_name>
	scratchDir := filepath.Join(common.Conf.Pkgs_dir, p.name)

	alreadyInstalled := false
	if _, err := os.Stat(scratchDir); err == nil { // 判断 scratchDir 是否存在
		// assume dir exististence means it is installed already
		// 如果 Pkg 的目录存在，那么其至少安装过一次(包括该 Pkg 依赖的其他 Pkgs)，直接返回
		// 跳过后续创建容器并传入 alreadyInstalled = true 这一部分冗余逻辑
		log.Printf("[packagePuller.go sandboxInstall()] pkg[%s] appears already installed from previous run of OL", p.name)
		alreadyInstalled = true
		return nil
	} else {
		// 如果不存在则创建相应的 scratchDir 目录
		if err := os.Mkdir(scratchDir, 0700); err != nil {
			return err
		}
	}

	defer func() {
		if err != nil {
			os.RemoveAll(scratchDir)
		}
	}()

	//sb := pp.sb
	sbMeta, err := pp.lmgr.GetSB("pkg-" + p.name)
	if err != nil {
		log.Printf("[packagePuller.go sandboxInstall()] failed to get a sb: %v\n", err)
		return
	}
	defer pp.lmgr.RecycleSB(sbMeta)

	sb := sbMeta.sb
	//log.Printf("[packagePuller.go sandboxInstall()] get sb[%d]\n", sbMeta.id)

	proxy, err := sb.HttpProxy()
	if err != nil {
		return nil
	}

	pipeFile := filepath.Join(sbMeta.scratchDir, "server_pipe")
	pipe, err := os.OpenFile(pipeFile, os.O_RDWR, 0777) // 以读写模式打开命名管道文件
	if err != nil {
		log.Printf("[packagePuller.go sandboxInstall()] cannot open server_pipe: %v\n", err)
		return
	}
	defer pipe.Close()
	
	fname := []byte("installLambda") // LambdaFunc.name 保存了请求名(/run/请求名)
	if _, err = pipe.Write(fname); err != nil {
		log.Printf("[packagePuller.go sandboxInstall()] failed to write filename to server_pipe\n")
	}

	// we still need to run a Sandbox to parse the dependencies, even if it is already installed
	msg := fmt.Sprintf(`{"pkg": "%s", "alreadyInstalled": %v}`, p.name, alreadyInstalled)
	reqBody := bytes.NewReader([]byte(msg))
	// the URL doesn't matter, since it is local anyway
	req, err := http.NewRequest("POST", "http://container/run/pip-install", reqBody)
	if err != nil {
		return err
	}
	log.Printf("[packagePuller.go sandboxInstall()] pkg[%s] install request sent at %v\n", p.name, time.Now().UnixNano() / 1e6)

	resp, err := proxy.Transport.RoundTrip(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	log.Printf("[packagePuller.go sandboxInstall()] pkg[%s] install response recv at %v\n", p.name, time.Now().UnixNano() / 1e6)

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		// did we run out of memory?
		if stat, err := sb.Status(sandbox.StatusMemFailures); err == nil {
			if b, err := strconv.ParseBool(stat); err == nil && b {
				return fmt.Errorf("ran out of memory while installing %s", p.name)
			}
		}

		return fmt.Errorf("install lambda returned status %d, body '%s'", resp.StatusCode, string(body))
	}

	if err := json.Unmarshal(body, &p.meta); err != nil {
		return err
	}

	//log.Printf("[packagePuller.go sandboxInstall()] pkg[%s] install result:< [Deps] [TopLevel]: '%s' >", p.name, p.meta)

	for i, pkg := range p.meta.Deps {
		p.meta.Deps[i] = normalizePkg(pkg)
	}

	return nil
}