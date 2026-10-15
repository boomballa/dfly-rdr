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
	"net/http"

	"github.com/julienschmidt/httprouter"
)

func index(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	for key := range counters.Items() {
		http.Redirect(w, r, "/instance/"+key.(string), http.StatusFound)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`<!DOCTYPE html><html><head><meta http-equiv="refresh" content="3"><title>Parsing RDB...</title><style>body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;text-align:center;padding:80px 20px;background:#f5f7fa;color:#333;}.box{max-width:500px;margin:0 auto;background:#fff;padding:40px;border-radius:12px;box-shadow:0 4px 20px rgba(0,0,0,0.08);}.spinner{border:4px solid #f3f3f3;border-top:4px solid #3498db;border-radius:50%;width:40px;height:40px;animation:spin 1s linear infinite;margin:20px auto;}@keyframes spin{0%{transform:rotate(0deg);}100%{transform:rotate(360deg);}}</style></head><body><div class="box"><div class="spinner"></div><h2>RDB 解析中...</h2><p style="color:#666;">大文件解析需要一定时间，页面每 3 秒自动刷新检测，解析完成后将自动进入看板。</p></div></body></html>`))
}

func rdbReveal(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	// deep copy  tplCommonData into data
	data := map[string]interface{}{}
	for key, val := range tplCommonData {
		data[key] = val
	}

	path := p.ByName("path")

	c := counters.Get(path)
	if c == nil {
		return
	}
	counter := c.(*Counter)

	data["CurrentInstance"] = path
	data["LargestKeys"] = counter.GetLargestEntries(100)

	largestKeyPrefixesByType := map[string][]*PrefixEntry{}
	for _, entry := range counter.GetLargestKeyPrefixes() {
		// mem use less than 1M, and list is long enough, not necessary to add
		if entry.Bytes < 1000*1000 && len(largestKeyPrefixesByType[entry.Type]) > 50 {
			continue
		}
		largestKeyPrefixesByType[entry.Type] = append(largestKeyPrefixesByType[entry.Type], entry)
	}
	data["LargestKeyPrefixes"] = largestKeyPrefixesByType

	data["TypeBytes"] = counter.typeBytes
	data["TypeNum"] = counter.typeNum
	totleNum := uint64(0)
	for _, v := range counter.typeNum {
		totleNum += v
	}
	totleBytes := uint64(0)
	for _, v := range counter.typeBytes {
		totleBytes += v
	}
	data["TotleNum"] = totleNum
	data["TotleBytes"] = totleBytes

	lenLevelCount := map[string][]*PrefixEntry{}
	for _, entry := range counter.GetLenLevelCount() {
		lenLevelCount[entry.Type] = append(lenLevelCount[entry.Type], entry)
	}
	data["LenLevelCount"] = lenLevelCount
	ServeHTML(w, "base.html", "revel.html", data)
}
