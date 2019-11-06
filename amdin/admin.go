package amdin

import (
	"fmt"
	"log"
	"net/http"
	"redis_protector/server"
	"sort"
	"strconv"
	"strings"
)

type AdminServer struct {
	port       int
	ctrlServer server.Server
}

func NewAdminServer(port int, controllerSrv server.Server) *AdminServer {
	return &AdminServer{port: port, ctrlServer: controllerSrv}
}

func (s *AdminServer) Start() {
	http.HandleFunc("/report", func(writer http.ResponseWriter, request *http.Request) {
		stats := s.ctrlServer.GetReport()
		sort.Sort(QueueStats(stats))
		var b strings.Builder
		b.WriteString("<table>")
		b.WriteString("<tr><td>队列名称</td><td>队列实际长度</td><td>借用队列长度</td><td>波动</td></tr>")
		template := "<tr><td>%s</td><td>%d</td><td>%d</td><td>%s</td></tr>"
		for _, v := range stats {
			adj := strconv.Itoa(int(float64(v.MasterLen-v.ExpectLen)/float64(v.MasterLen)*100)) + "%"
			b.WriteString(fmt.Sprintf(template, v.QueueName, v.MasterLen, v.SlaveLen, adj))
		}
		b.WriteString("</table>")
		writer.Write([]byte(b.String()))
	})
	go func() {
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", s.port), nil))
	}()
}

type QueueStats []server.QueueStat

func (q QueueStats) Len() int {
	return len(q)
}

func (q QueueStats) Less(i, j int) bool {
	return q[i].QueueName < q[j].QueueName
}

func (q QueueStats) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}
