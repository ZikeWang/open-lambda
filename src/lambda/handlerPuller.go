package lambda

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	//"log"

	"github.com/open-lambda/open-lambda/ol/common"
)

var notFound404 = errors.New("file does not exist")

// TODO: for web registries, support an HTTP-based access key
// (https://en.wikipedia.org/wiki/Basic_access_authentication)

type HandlerPuller struct {
	prefix   string   // combine with name to get file path or URL
	dirCache sync.Map // key=lambda name, value=version, directory path
	dirMaker *common.DirMaker
}

type CacheEntry struct {
	version string // could be a timestamp for a file or web resource
	path    string // where code is extracted to a dir
}

func NewHandlerPuller(dirMaker *common.DirMaker) (cp *HandlerPuller, err error) {
	return &HandlerPuller{
		prefix:   common.Conf.Registry,
		dirMaker: dirMaker,
	}, nil
}

func (cp *HandlerPuller) isRemote() bool {
	return strings.HasPrefix(cp.prefix, "http://") || strings.HasPrefix(cp.prefix, "https://")
}

func (cp *HandlerPuller) Pull(name string) (targetDir string, err error) {
	t := common.T0("pull-lambda")
	defer t.T1()

	matched, err := regexp.MatchString(`^[A-Za-z0-9\.\-\_]+$`, name)
	if err != nil {
		return "", err
	} else if !matched {
		msg := "bad lambda name '%s', can only contain letters, numbers, period, dash, and underscore"
		return "", fmt.Errorf(msg, name)
	}

	if cp.isRemote() {
		// registry type = web
		urls := []string{
			cp.prefix + "/" + name + ".tar.gz",
			cp.prefix + "/" + name + ".py",
		}

		for i := 0; i < len(urls); i++ {
			targetDir, err = cp.pullRemoteFile(urls[i], name)
			if err == nil {
				return targetDir, nil
			} else if err != notFound404 {
				// 404 is OK, because we just go on to check the next URLs
				return "", err
			}
		}

		return "", fmt.Errorf("lambda not found at any of these locations: %s", strings.Join(urls, ", "))
	} else {
		// registry type = file
		paths := []string{ // prefix 为 common.Conf.Resgistry 即 open-lambda/test-registry
			filepath.Join(cp.prefix, name) + ".tar.gz", // tar file: xxx.tar.gz
			filepath.Join(cp.prefix, name) + ".py", // regular file: xxx.py
			filepath.Join(cp.prefix, name), // dir: xxx
		}

		for i := 0; i < len(paths); i++ {
			if _, err := os.Stat(paths[i]); !os.IsNotExist(err) { // paths[i] 对应路径的文件存在则执行 if 分支
				//log.Printf("[handlerPuller.go 86] paths[%d] '%s' exists and ready to pull from there\n", i, paths[i])
				targetDir, err = cp.pullLocalFile(paths[i], name) // 传入完整路径和文件名
				return targetDir, err
			}
		}

		return "", fmt.Errorf("lambda not found at any of these locations: %s", strings.Join(paths, ", "))
	}
}

// delete any caching associated with this handler
func (cp *HandlerPuller) Reset(name string) {
	cp.dirCache.Delete(name)
}

func (cp *HandlerPuller) pullLocalFile(src, lambdaName string) (targetDir string, err error) {
	//log.Printf("[handlerPuller.go 102] pullLocalFile src path is '%s'\n", src)
	stat, err := os.Stat(src) // Stat returns type FileInfo
	if err != nil {
		return "", err
	}

	if stat.Mode().IsDir() {
		// this is really just a debug mode, and is not
		// expected to be efficient
		targetDir = cp.dirMaker.Get(lambdaName) // open-lambda/test-dir/worker/code/1001-echo

		cmd := exec.Command("cp", "-r", src, targetDir) // -r：若给出的源文件是一个目录文件，复制该目录下所有的子目录和文件到目的目录下
		if output, err := cmd.CombinedOutput(); err != nil {
			return "", fmt.Errorf("%s :: %s", err, string(output))
		}
		return targetDir, nil // 对于 open-lambda 原始的实现(test-registry/echo/f.py) 在这里返回
	} else if !stat.Mode().IsRegular() {
		return "", fmt.Errorf("%s not a file or directory", src)
	}

	// for regular files, we cache based on mod time.  We don't
	// cache at the file level if this is a remote store (because
	// caching is handled at the web level)
	version := stat.ModTime().String() // Modification Time
	if !cp.isRemote() {
		cacheEntry := cp.getCache(lambdaName)
		if cacheEntry != nil && cacheEntry.version == version {
			// hit:
			return cacheEntry.path, nil
		}
	}

	// miss:
	targetDir = cp.dirMaker.Get(lambdaName) // HandlerPuller.dirMaker 实际为 LambdaMgr.codeDirs, open-lambda/test-dir/worker/code/1001-echo
	if err := os.Mkdir(targetDir, os.ModeDir); err != nil { // 根据 targetDir 路径创建目录：open-lambda/test-dir/worker/code/1001-echo
		return "", err
	}

	if strings.HasSuffix(src, ".py") { // open-lambda/test-registry/echo
		cmd := exec.Command("cp", src, filepath.Join(targetDir, "f.py"))
		if output, err := cmd.CombinedOutput(); err != nil {
			return "", fmt.Errorf("%s :: %s", err, string(output))
		}
	} else if strings.HasSuffix(src, ".tar.gz") {
		cmd := exec.Command("tar", "-xzf", src, "--directory", targetDir)
		if output, err := cmd.CombinedOutput(); err != nil {
			return "", fmt.Errorf("%s :: %s", err, string(output))
		}
	} else {
		return "", fmt.Errorf("lambda file %s not a .ta.rgz or .py", src)
	}

	if !cp.isRemote() {
		cp.putCache(lambdaName, version, targetDir)
	}

	return targetDir, nil
}

func (cp *HandlerPuller) pullRemoteFile(src, lambdaName string) (targetDir string, err error) {
	// grab latest lambda code if it's changed (pass
	// If-Modified-Since so this can be determined on server side
	client := &http.Client{}
	req, err := http.NewRequest("GET", src, nil)
	if err != nil {
		return "", err
	}

	cacheEntry := cp.getCache(lambdaName)
	if cacheEntry != nil {
		req.Header.Set("If-Modified-Since", cacheEntry.version)
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", notFound404
	}

	if resp.StatusCode == http.StatusNotModified {
		return cacheEntry.path, nil
	}

	// download to local file, then use pullLocalFile to finish
	dir, err := ioutil.TempDir("", "ol-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(dir)

	parts := strings.Split(src, "/")
	localPath := filepath.Join(dir, parts[len(parts)-1])
	out, err := os.Create(localPath)
	if err != nil {
		return "", err
	}
	defer out.Close()

	if _, err = io.Copy(out, resp.Body); err != nil {
		return "", err
	}

	targetDir, err = cp.pullLocalFile(localPath, lambdaName)

	// record directory in cache, by mod time
	if err == nil {
		version := resp.Header.Get("Last-Modified")
		if version != "" {
			cp.putCache(lambdaName, version, targetDir)
		}
	}

	return targetDir, err
}

func (cp *HandlerPuller) getCache(name string) *CacheEntry {
	entry, found := cp.dirCache.Load(name) // load 返回 sync.map 中 key(name) 对应的 value(&CacheEntry{version, path})，如果没有则返回 nil
	if !found {
		return nil
	}
	return entry.(*CacheEntry)
}

func (cp *HandlerPuller) putCache(name, version, path string) {
	cp.dirCache.Store(name, &CacheEntry{version, path})
}
