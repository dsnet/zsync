// Copyright 2017, The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE.md file.

package main

import (
	"bytes"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dsnet/golib/unitconv"
)

func (zs *zsyncer) ServeHTTP() {
	zs.log.Printf("starting http server")
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")

		var bb bytes.Buffer
		defer func() { w.Write(bb.Bytes()) }()

		bb.WriteString(`
<html>
<head>
	<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
	<title>ZSync Dashboard</title>
	<style>body { font-family: monospace; } th, td { padding: 5px; } table, th, td { border: 1px solid black; }</style>
</head>
<body>
`[1:])
		defer bb.WriteString("</body>\n</html>\n")

		// Print pool statuses.
		{
			table := [][]string{{"Pool", "Status"}}
			styles := make(map[[2]int]string)
			var pools []string
			for pool := range zs.poolMonitors {
				pools = append(pools, pool)
			}
			sort.Strings(pools)
			for _, pool := range pools {
				var state, style string
				switch zs.poolMonitors[pool].Status().State {
				case +2:
					state = "✅ HEALTHY"
					style = "background-color:#d0ffd0;" // green
				case +1:
					state = "✅ HEALTHY"
					style = "background-color:#ffffd0;" // yellow
				case -1:
					state = "❌ UNHEALTHY"
					style = "background-color:#ffffd0;" // yellow
				case -2:
					state = "❌ UNHEALTHY"
					style = "background-color:#ffd0d0;" // red
				default:
					state = "❓ UNKNOWN"
					style = "background-color:#ffffd0;" // yellow
				}
				styles[[2]int{len(table), 1}] = style
				table = append(table, []string{pool, state})

			}
			writeTable(&bb, table, styles)
		}

		io.WriteString(&bb, "<br>\n")

		// Print snapshot statuses.
		{
			table := [][]string{{"Dataset", "Latest Snapshot"}}
			styles := make(map[[2]int]string)
			var ids []string
			for id := range zs.snapshotManagers {
				ids = append(ids, id)
			}
			sort.Strings(ids)
			for _, id := range ids {
				sm := zs.snapshotManagers[id]

				srcLatest := sm.srcDataset.latestSnapshot.Load()
				switch srcLatest {
				case "":
					styles[[2]int{len(table), 1}] = "background-color:#ffffd0;" // yellow
					table = append(table, []string{id, "❓ UNKNOWN"})
				default:
					styles[[2]int{len(table), 1}] = "background-color:#d0ffd0;" // green
					table = append(table, []string{id, "✅ " + srcLatest})
				}

				for i, dst := range sm.dstDatasets {
					label := "├── " + dst.DatasetPath()
					if i == len(sm.dstDatasets)-1 {
						label = "└── " + dst.DatasetPath()
					}
					dstLatest := dst.latestSnapshot.Load()
					switch dstLatest {
					case "":
						styles[[2]int{len(table), 1}] = "background-color:#ffffd0;" // yellow
						table = append(table, []string{label, "❓ UNKNOWN"})
					case srcLatest:
						styles[[2]int{len(table), 1}] = "background-color:#d0ffd0;" // green
						table = append(table, []string{label, "✅ " + dstLatest})
					default:
						styles[[2]int{len(table), 1}] = "background-color:#ffd0d0;" // red
						table = append(table, []string{label, "❌ " + dstLatest})
					}
				}
			}
			writeTable(&bb, table, styles)
		}

		io.WriteString(&bb, "<br>\n")

		// Print replication statuses.
		{
			table := [][]string{{"Started", "Source", "Destination", "Transferred", "Status"}}
			styles := make(map[[2]int]string)
			var ids []string
			for id := range zs.replicaManagers {
				ids = append(ids, id)
			}
			sort.Strings(ids)
			for _, source := range ids {
				for i, status := range zs.replicaManagers[source].Status() {
					var started, transferred, state, style string
					destination := zs.replicaManagers[source].dstDatasets[i].DatasetPath()
					if !status.Started.IsZero() {
						started = formatDate(status.Started)
						var duration time.Duration
						if status.Finished.IsZero() {
							duration = time.Now().Sub(status.Started)
						} else {
							duration = status.Finished.Sub(status.Started)
						}
						transferred = unitconv.FormatPrefix(float64(status.Transferred), unitconv.IEC, 2) + "B at " +
							unitconv.FormatPrefix(float64(status.Transferred)/duration.Seconds(), unitconv.IEC, 2) + "B/s"
						switch {
						case status.Finished.IsZero():
							state = "❗ COPYING"
							style = "background-color:#ffffd0;" // yellow
						case status.FaultReason != "":
							state = "❌ FAULT<pre>" + status.FaultReason + "</pre>"
							style = "background-color:#ffd0d0;" // red
						default:
							state = "✅ FINISHED"
							style = "background-color:#d0ffd0;" // green
						}
					}
					styles[[2]int{len(table), 4}] = style
					table = append(table, []string{started, source, destination, transferred, state})
				}
			}
			writeTable(&bb, table, styles)
		}
	})
	for {
		if err := http.ListenAndServe(zs.http.Address, nil); err != nil {
			zs.log.Printf("http.ListenAndServe error: %v", err)
			time.Sleep(30 * time.Second)
		}
	}
}

func writeTable(w io.Writer, table [][]string, styles map[[2]int]string) {
	io.WriteString(w, "<table>\n<tbody>\n")
	defer io.WriteString(w, "</tbody>\n</table>\n")
	for i, row := range table {
		tag := "th"
		if i > 0 {
			tag = "td"
		}
		func() {
			io.WriteString(w, "<tr>\n")
			defer io.WriteString(w, "</tr>\n")
			for j, column := range row {
				style := styles[[2]int{i, j}]
				if style != "" {
					style = ` style="` + style + `"`
				}
				io.WriteString(w, "<"+tag+style+">"+column+"</"+tag+">\n")
			}
		}()
	}
}

func formatDate(t time.Time) string {
	suffix := "th"
	switch t.Day() {
	case 1, 21, 31:
		suffix = "st"
	case 2, 22:
		suffix = "nd"
	case 3, 23:
		suffix = "rd"
	}
	var ago string
	switch d := time.Now().Sub(t); {
	case d < time.Second:
		ago = d.Round(time.Millisecond).String()
	case d < time.Minute:
		ago = d.Round(time.Second).String()
	case d < time.Hour:
		ago = strings.TrimSuffix(d.Round(time.Minute).String(), "0s")
	case d < 24*time.Hour:
		ago = strings.TrimSuffix(d.Round(time.Hour).String(), "0m0s")
	default:
		ago = strconv.Itoa(int(d.Hours()/24)) + "days"
	}
	return t.Format("Jan 2"+suffix+", 3:04pm") + " (" + ago + " ago)"
}
