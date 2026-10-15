// Copyright 2017 XUEQIU.COM
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dump

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"syscall"
	"time"

	assetfs "github.com/elazarl/go-bindata-assetfs"
	"github.com/julienschmidt/httprouter"
	"github.com/urfave/cli"
	"github.com/xueqiu/rdr/decoder"
	"github.com/xueqiu/rdr/static"
)

var counters = NewSafeMap()

func listPathFiles(pathname string) []string {
	var filenames []string
	fi, err := os.Lstat(pathname) // For read access.
	if err != nil {
		return filenames
	}
	if fi.IsDir() {
		files, err := ioutil.ReadDir(pathname)
		if err != nil {
			log.Fatal(err)
		}
		for _, f := range files {
			name := path.Join(pathname, f.Name())
			filenames = append(filenames, name)
		}
	} else {
		filenames = append(filenames, pathname)
	}
	return filenames
}

// Show parse rdbfile(s) and show statistical information by html
func Show(c *cli.Context) {
	if c.NArg() < 1 {
		fmt.Fprintln(c.App.ErrWriter, "show requires at least 1 argument")
		cli.ShowCommandHelp(c, "show")
		return
	}

	// 1. 忽略 SIGHUP 信号，防止终端断开或未加 nohup 运行时被系统挂断信号杀死
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGHUP)
	go func() {
		for range sigChan {
			// 忽略终端挂断信号，保障后台服务常驻
		}
	}()

	// parse rdbfile
	fmt.Fprintln(c.App.Writer, "start parsing...")
	instances := []string{}
	InitHTMLTmpl()

	// 2. 同步解析初始指定的文件/目录，确保解析真正完成后再启动服务并输出 finished
	for _, pathname := range c.Args() {
		for _, v := range listPathFiles(pathname) {
			filename := filepath.Base(v)

			if !counters.Check(filename) {
				decoder := decoder.NewDecoder()
				fmt.Fprintf(c.App.Writer, "start to parse %v \n", filename)
				go Decode(c, decoder, v)
				counter := NewCounter()
				counter.Count(decoder.Entries)
				counters.Set(filename, counter)
				fmt.Fprintf(c.App.Writer, "parse %v  done\n", filename)

				instances = append(instances, filename)
			}
		}
	}
	tplCommonData["Instances"] = instances

	// 3. 解析完成后主动执行 GC 并将中间解压/解码临时内存归还给操作系统，防止触发系统 OOM-Killer
	runtime.GC()
	debug.FreeOSMemory()

	// 4. 后台持续监控目录增量文件（如果是传入目录）
	go func() {
		for {
			time.Sleep(5 * time.Second)
			for _, pathname := range c.Args() {
				for _, v := range listPathFiles(pathname) {
					filename := filepath.Base(v)

					if !counters.Check(filename) {
						decoder := decoder.NewDecoder()
						fmt.Fprintf(c.App.Writer, "start to parse %v \n", filename)
						go Decode(c, decoder, v)
						counter := NewCounter()
						counter.Count(decoder.Entries)
						counters.Set(filename, counter)
						fmt.Fprintf(c.App.Writer, "parse %v  done\n", filename)

						instances = append(instances, filename)
						tplCommonData["Instances"] = instances
					}
				}
			}
		}
	}()

	// 5. 启动 http 报表服务并输出完成提示
	staticFS := assetfs.AssetFS{
		Asset:     static.Asset,
		AssetDir:  static.AssetDir,
		AssetInfo: static.AssetInfo,
	}
	router := httprouter.New()
	router.ServeFiles("/static/*filepath", &staticFS)
	router.GET("/", index)
	router.GET("/instance/:path", rdbReveal)

	port := c.String("port")
	fmt.Fprintf(c.App.Writer, "parsing finished, please access http://{$IP}:%s\n", port)
	listenErr := http.ListenAndServe(":"+port, router)
	if listenErr != nil {
		fmt.Fprintf(c.App.ErrWriter, "\n[FATAL ERROR] http.ListenAndServe failed on port %s: %v\n", port, listenErr)
		log.Fatalf("Listen port %s error: %v", port, listenErr)
	}
}
