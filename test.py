#!/usr/bin/env python3
import os, sys, json, time, requests, copy, traceback, tempfile, threading, subprocess
from collections import OrderedDict
from subprocess import check_output
from multiprocessing import Pool
from contextlib import contextmanager        
                
OLDIR = 'test-dir'

results = OrderedDict({"runs": []})
curr_conf = None


def post(path, data=None):
    return requests.post('http://localhost:5000/'+path, json.dumps(data)) 
    # dumps将将Python对象编码成JSON字符串，然后发送post(url, data)请求


def raise_for_status(r):
    if r.status_code != 200:
        raise Exception("STATUS %d: %s" % (r.status_code, r.text))
        # 如果返回的http状态码不是200(即成功处理了请求)，通过raise显式的触发异常


def test_in_filter(name):
    if len(sys.argv) < 2: # sys.argv[]用来获取命令行参数的，sys.argv[0]表示代码本身文件路径，所以参数从1开始为实际命令所带的参数
        return True # 表示命令不带参数
    return name in sys.argv[1:] # 如果参数name存在于切片sys.argv[1:]中则表达式为True


def get_mem_stat_mb(stat):
    with open('/proc/meminfo') as f:
        for l in f:
            if l.startswith(stat+":"):
                parts = l.strip().split()
                assert(parts[-1] == 'kB')
                return int(parts[1]) / 1024
    raise Exception('could not get stat')

# 通过get_mem_stat_mb函数获取 “当前可用内存” 数据，超过函数中静态定义的限制后，执行系统调用
def ol_oom_killer():
    while True:
        if get_mem_stat_mb('MemAvailable') < 128:
            print("out of memory, trying to kill OL")
            os.system('pkill ol') # os.system() 用于执行传入参数的指令，代替了手动在命令行输入
        time.sleep(1)

def test(fn):
    def wrapper(*args, **kwargs):
        if len(args):
            raise Exception("positional args not supported for tests")

        name = fn.__name__ # 获取传入的函数的名称

        if not test_in_filter(name): # 当函数返回false时执行，即 len(sys.argv)>=2 && (name in sys.argv[1:])==false
            return None # 也即：当命令显式的带有参数，而名为name的测试不在参数中，则不执行该name的测试

		# 当命令行不带任何参数（执行所有测试），或当前name对应的函数名在参数列表时，往下执行

        print('='*40)
        if len(kwargs):
            print(name, kwargs)
        else:
            print(name)
        print('='*40)
        result = OrderedDict()
        result["test"] = name
        result["params"] = kwargs
        result["pass"] = None
        result["conf"] = curr_conf
        result["seconds"] = None
        result["total_seconds"] = None
        result["stats"] = None
        result["ol-stats"] = None
        result["errors"] = []
        result["worker_tail"] = None

        total_t0 = time.time()
        mounts0 = mounts()
        try:
            # setup worker
            run(['./ol', 'worker', '-p='+OLDIR, '--detach'])

            # run test/benchmark
            test_t0 = time.time()
            rv = fn(**kwargs)
            test_t1 = time.time() # 截至这里记录的是测试函数的执行时间
            result["seconds"] = test_t1 - test_t0

            result["pass"] = True
        except Exception: # 该except子句的名字为Exception，匹配L51的Exception
            rv = None
            result["pass"] = False
            result["errors"].append(traceback.format_exc().split("\n"))

        # cleanup worker
        try:
            run(['./ol', 'kill', '-p='+OLDIR])
        except Exception:
            result["pass"] = False
            result["errors"].append(traceback.format_exc().split("\n"))
        mounts1 = mounts()
        if len(mounts0) != len(mounts1):
            result["pass"] = False
            result["errors"].append(["mounts are leaking (%d before, %d after), leaked: %s"
                                     % (len(mounts0), len(mounts1), str(mounts1 - mounts0))])

        # get internal stats from OL
        if os.path.exists(OLDIR+"/worker/stats.json"): # 用于判断括号里文件路径对应的文件是否存在
            with open(OLDIR+"/worker/stats.json") as f:
                olstats = json.load(f) # 将json格式（或者直接说是字符串）数据转换为字典
                result["ol-stats"] = OrderedDict(sorted(list(olstats.items())))

        total_t1 = time.time() # 截至这里记录的是包括了测试函数的运行、执行后环境的清理、状态的获取等完整流程的总时间
        result["total_seconds"] = total_t1-total_t0
        result["stats"] = rv

        with open(os.path.join(OLDIR, "worker.out")) as f:
            result["worker_tail"] = f.read().split("\n")
            if result["pass"]:
                # truncate because we probably won't use it for debugging
                result["worker_tail"] = result["worker_tail"][-10:] # 从倒数第10个到最后，含头含尾；如果是[-10,-1]则包含最后一个

        results["runs"].append(result)
        print(json.dumps(result, indent=2))
        return rv

    return wrapper

# 该函数将传入的参数写进 OLDIR/config.json ，并将curr_conf设置为该参数
def put_conf(conf):
    global curr_conf # 使用global关键字后可以对该全局变量进行修改
    with open(os.path.join(OLDIR, "config.json"), "w") as f: # w为覆盖写模式
        json.dump(conf, f, indent=2) # dump()与dumps()还是有点区别的，dump用于将dict类型的数据(第一个参数)转成str，并写入到json文件中(第二个参数)
    curr_conf = conf

# 该函数用于把命令行mount命令执行后的结果存储到变量中
def mounts():
    output = check_output(["mount"]) # subprocess.check_output函数会使用参数作为命令来运行，并返回命令执行后的输出结果。通过 mount 命令可以查看已挂载的文件系统，会输出丰富的信息
    output = str(output, "utf-8") # 输出的结果不一定是string，所以要显式的转换
    output = output.split("\n") # split函数指定了以 "\n" 为分隔符对字符串进行切片，即将原字符串类似于 "xxx\nxxx\nxxx\n" 的形式转换为 {'xxx', 'xxx', 'xxx'}
    return set(output) # set() 函数创建一个无序不重复元素的集合

        
@contextmanager
def TestConf(**keywords): # 关键词参数，是python中的可变参数，本质上是一个 dict，如 a=1
    with open(os.path.join(OLDIR, "config.json")) as f:
        orig = json.load(f) # 结果为字典
    new = copy.deepcopy(orig) # 深复制是将被复制对象完全再复制一遍作为独立的新个体单独存在。所以改变原有被复制对象不会对已经复制出来的新对象产生影响。
    for k in keywords: # 遍历字典时遍历的是键
        if not k in new:
            raise Exception("unknown config param: %s" % k)
        if type(keywords[k]) == dict: # 键对应的值也是字典，即keywords为嵌套的字典。
            for k2 in keywords[k]:
                new[k][k2] = keywords[k][k2] # 目的是将config.json中与dict参数键对应的配置项的值改为参数的值
        else:
            new[k] = keywords[k] # 同上

    # setup
    print("PUSH conf:", keywords)
    put_conf(new) # 通过覆盖写新实现前面的修改配置文件的目的

    yield new

    # cleanup
    print("POP conf:", keywords)
    put_conf(orig) # 通过将原始数据写回实现撤销修改clean回原状态


def run(cmd):
    print("RUN", " ".join(cmd))
    try:
        out = check_output(cmd, stderr=subprocess.STDOUT)
        fail = False
    except subprocess.CalledProcessError as e:
        out = e.output
        fail = True

    out = str(out, 'utf-8')
    if len(out) > 500:
        out = out[:500] + "..."

    if fail:
        raise Exception("command (%s) failed: %s"  % (" ".join(cmd), out))


@test
def install_tests():
    # we want to make sure we see the expected number of pip installs,
    # so we don't want installs lying around from before
    rc = os.system('rm -rf test-dir/lambda/packages/*') # os.system(cmd)用于在python中调用linux命令。执行成功返回0，否则返回一个错误码
    assert(rc == 0)

    # try something that doesn't install anything
    msg = 'hello world'
    r = post("run/echo", msg)
    raise_for_status(r)
    if r.json() != msg:
        raise Exception("found %s but expected %s" % (r.json(), msg))
    r = post("stats", None)
    raise_for_status(r)
    installs = r.json().get('pull-package.cnt', 0)
    assert(installs == 0)

    for i in range(3):
        name = "install"
        if i != 0:
            name += str(i+1)
        r = post("run/"+name, {})
        raise_for_status(r)
        assert r.json() == "imported"

        r = post("stats", None)
        raise_for_status(r)
        installs = r.json()['pull-package.cnt']
        if i < 2:
            # with deps, requests should give us these:
            # certifi, chardet, idna, requests, urllib3
            assert(installs == 5)            
        else:
            assert(installs == 6)


@test
def numpy_test():
    # try adding the nums in a few different matrixes.  Also make sure
    # we can have two different numpy versions co-existing.
    r = post("run/numpy15", [1, 2])
    if r.status_code != 200:
        raise Exception("STATUS %d: %s" % (r.status_code, r.text))
    j = r.json()
    assert j['result'] == 3
    assert j['version'].startswith('1.15')

    r = post("run/numpy16", [[1, 2], [3, 4]])
    if r.status_code != 200:
        raise Exception("STATUS %d: %s" % (r.status_code, r.text))
    j = r.json()
    assert j['result'] == 10
    assert j['version'].startswith('1.16')

    r = post("run/numpy15", [[[1, 2], [3, 4]], [[1, 2], [3, 4]]])
    if r.status_code != 200:
        raise Exception("STATUS %d: %s" % (r.status_code, r.text))
    j = r.json()
    assert j['result'] == 20
    assert j['version'].startswith('1.15')

    r = post("run/pandas15", [[1, 2, 3],[1, 2, 3]])
    if r.status_code != 200:
        raise Exception("STATUS %d: %s" % (r.status_code, r.text))
    j = r.json()
    assert j['result'] == 12
    assert j['version'].startswith('1.15')

    r = post("run/pandas", [[0, 1, 2], [3, 4, 5]])
    if r.status_code != 200:
        raise Exception("STATUS %d: %s" % (r.status_code, r.text))
    j = r.json()
    assert j['result'] == 15
    assert float(".".join(j['version'].split('.')[:2])) >= 1.16


def stress_one_lambda_task(args):
    t0, seconds = args
    i = 0
    while time.time() < t0 + seconds:
        r = post("run/echo", i)
        raise_for_status(r)
        assert r.text == str(i)
        i += 1
    return i


@test
def stress_one_lambda(procs, seconds):
    t0 = time.time()

    with Pool(procs) as p:
        reqs = sum(p.map(stress_one_lambda_task, [(t0, seconds)] * procs, chunksize=1))

    return {"reqs_per_sec": reqs/seconds}


@test
def call_each_once_exec(lambda_count, alloc_mb):
    # TODO: do in parallel
    t0 = time.time()
    for i in range(lambda_count):
        r = post("run/L%d"%i, {"alloc_mb": alloc_mb})
        raise_for_status(r)
        assert r.text == str(i)
    seconds = time.time() - t0

    return {"reqs_per_sec": lambda_count/seconds}


def call_each_once(lambda_count, alloc_mb=0):
    with tempfile.TemporaryDirectory() as reg_dir:
        # create dummy lambdas
        for i in range(lambda_count):
            with open(os.path.join(reg_dir, "L%d.py"%i), "w") as f:
                f.write("def f(event):\n")
                f.write("    global s\n")
                f.write("    s = '*' * %d * 1024**2\n" % alloc_mb)
                f.write("    return %d\n" % i)

        with TestConf(registry=reg_dir):
            call_each_once_exec(lambda_count=lambda_count, alloc_mb=alloc_mb)


@test
def fork_bomb():
    limit = curr_conf["limits"]["procs"]
    r = post("run/fbomb", {"times": limit*2})
    raise_for_status(r)
    # the function returns the number of children that we were able to fork
    actual = int(r.text)
    assert(1 <= actual <= limit)


@test
def max_mem_alloc():
    limit = curr_conf["limits"]["mem_mb"]
    r = post("run/max_mem_alloc", None)
    raise_for_status(r)
    # the function returns the MB that was able to be allocated
    actual = int(r.text)
    assert(limit-16 <= actual <= limit)


@test
def ping_test():
    pings = 1000
    t0 = time.time()
    for i in range(pings):
        r = requests.get("http://localhost:5000/status")
        raise_for_status(r)
    seconds = time.time() - t0
    return {"pings_per_sec": pings/seconds}


def sock_churn_task(args):
    echo_path, parent, t0, seconds = args
    i = 0
    while time.time() < t0 + seconds:
        args = {"code": echo_path, "leaf": True, "parent": parent}
        r = post("create", args)
        raise_for_status(r)
        sandbox_id = r.text.strip()
        r = post("destroy/"+sandbox_id, {})
        raise_for_status(r)
        i += 1
    return i


@test
def sock_churn(baseline, procs, seconds, fork):
    # baseline: how many sandboxes are sitting idly throughout the experiment
    # procs: how many procs are concurrently creating and deleting other sandboxes

    echo_path = os.path.abspath("test-registry/echo")

    if fork:
        r = post("create", {"code": "", "leaf": False})
        raise_for_status(r)
        parent = r.text.strip()
    else:
        parent = ""

    for i in range(baseline):
        r = post("create", {"code": echo_path, "leaf": True, "parent": parent})
        raise_for_status(r)
        sandbox_id = r.text.strip()
        r = post("pause/"+sandbox_id)
        raise_for_status(r)

    t0 = time.time()
    with Pool(procs) as p:
        reqs = sum(p.map(sock_churn_task, [(echo_path, parent, t0, seconds)] * procs, chunksize=1))

    return {"sandboxes_per_sec": reqs/seconds}


@test
def update_code():
    reg_dir = curr_conf['registry']
    cache_seconds = curr_conf['registry_cache_ms'] / 1000
    latencies = []

    for i in range(3):
        # update function code
        with open(os.path.join(reg_dir, "version.py"), "w") as f:
            f.write("def f(event):\n")
            f.write("    return %d\n" % i)

        # how long does it take for us to start seeing the latest code?
        t0 = time.time()
        while True:
            r = post("run/version", None)
            raise_for_status(r)
            num = int(r.text)
            assert(num >= i-1)
            t1 = time.time()

            # make sure the time to grab new code is about the time
            # specified for the registry cache (within ~1 second)
            assert(t1 - t0 <= cache_seconds + 1)
            if num == i:
                if i > 0:
                    assert(t1 - t0 >= cache_seconds - 1)
                break


@test
def recursive_kill(depth):
    parent = ""
    for i in range(depth):
        r = post("create", {"code": "", "leaf": False, "parent": parent})
        raise_for_status(r)
        if parent:
            # don't need this parent any more, so pause it to get
            # memory back (so we can run this test with low memory)
            post("pause/"+parent)
        parent = r.text.strip()

    r = post("destroy/1", None)
    raise_for_status(r)
    r = post("stats", None)
    raise_for_status(r)
    destroys = r.json()['Destroy():ms.cnt']
    assert destroys == depth


def tests():
    test_reg = os.path.abspath("test-registry")

    with TestConf(registry=test_reg):
        ping_test()

        # do smoke tests under various configs
        with TestConf(features={"import_cache": False}):
            install_tests()
        with TestConf(mem_pool_mb=500):
            install_tests()
        with TestConf(sandbox="docker", features={"import_cache": False}):
            install_tests()

        # test resource limits
        fork_bomb()
        max_mem_alloc()

        # numpy pip install needs a larger mem cap
        with TestConf(mem_pool_mb=500):
            numpy_test()

    # test SOCK directly (without lambdas)
    with TestConf(server_mode="sock", mem_pool_mb=500):
        sock_churn(baseline=0, procs=1, seconds=5, fork=False)
        sock_churn(baseline=0, procs=1, seconds=10, fork=True)
        sock_churn(baseline=0, procs=15, seconds=10, fork=True)
        sock_churn(baseline=32, procs=1, seconds=10, fork=True)
        sock_churn(baseline=32, procs=15, seconds=10, fork=True)

    # make sure code updates get pulled within the cache time
    with tempfile.TemporaryDirectory() as reg_dir:
        with TestConf(registry=reg_dir, registry_cache_ms=3000):
            update_code()

    # test heavy load
    with TestConf(registry=test_reg):
        stress_one_lambda(procs=1, seconds=15)
        stress_one_lambda(procs=2, seconds=15)
        stress_one_lambda(procs=8, seconds=15)

    with TestConf(features={"reuse_cgroups": True}):
        call_each_once(lambda_count=100, alloc_mb=1)
        call_each_once(lambda_count=1000, alloc_mb=10)


def main():
    t0 = time.time()

    # so our test script doesn't hang if we have a memory leak
    timerThread = threading.Thread(target=ol_oom_killer, daemon=True)
    timerThread.start()

    # general setup
    if os.path.exists(OLDIR):
        try:
            run(['./ol', 'kill', '-p='+OLDIR])
        except:
            print('could not kill cluster')
        run(['rm', '-rf', OLDIR])
    run(['./ol', 'new', '-p='+OLDIR])

    # run tests with various configs
    with TestConf(limits={"installer_mem_mb": 250}):
        tests()

    # save test results
    passed = len([t for t in results["runs"] if t["pass"]])
    failed = len([t for t in results["runs"] if not t["pass"]])
    results["passed"] = passed
    results["failed"] = failed
    results["seconds"] = time.time() - t0
    print("PASSED: %d, FAILED: %d" % (passed, failed))

    with open("test.json", "w") as f:
        json.dump(results, f, indent=2)

    sys.exit(failed)


if __name__ == '__main__':
    main()
