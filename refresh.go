package main

import (
	"fmt"
	"iot"

	"log"
	"time"
)

func refresher() {

	refresherRunning = true
	nextRefresh = 0
	prevRefresh = time.Now().Unix()

	iot.Info.Printf("refresher: Start\n")

	for refresherRunning {

		if time.Now().Unix() >= nextRefresh {

			nextRefresh = time.Now().Unix() + 1

			//fmt.Printf("------- refresher -------\n")

			var err error

			stmt, errr := iot.DatabaseNew.Prepare(`
			SELECT nodeId, propId, nextRefresh FROM propsmem where nextRefresh <= ? and nextRefresh > 0 order by nextRefresh`)

			checkError(errr)

			defer stmt.Close()

			rows, errr := stmt.Query(time.Now().Unix())
			//fmt.Printf("refresh select < %d \n", time.Now().Unix())

			if errr != nil {
				log.Fatal(err)
			}

			defer rows.Close()

			cnt := 0

			var nodeID int
			var propID int
			var nextRefresh int64

			for rows.Next() {

				err := rows.Scan(&nodeID, &propID, &nextRefresh)
				checkError(err)

				cnt++

				prop := iot.GetProp(nodeID, propID)
				prop.RetryCount++

				if prop.RetryCount > 2 &&
					prop.RetryCount < 8 {
					iot.MvcUp(prop) // refresh UI when refreshing issues

				}

				if prop.RefreshRate < 1 {
					prop.NextRefresh = 0

				} else {

					if prop.RetryCount > 7 {
						prop.NextRefresh = time.Now().Unix() + int64(prop.RefreshRate)*7

					} else {
						prop.NextRefresh = time.Now().Unix() + int64(prop.RefreshRate)*int64(prop.RetryCount)

					}
				}

				iot.PersistPropToMem(prop)

				if iot.LogProp(prop) || iot.LogCode("R") {
					iot.Trace.Printf("refresh n%dp%d retries %d next %d\n", nodeID, propID, prop.RetryCount, prop.NextRefresh)
				}

				iot.SendProp(prop, "R", "")

				if !prop.Node.IsOffline &&
					prop.Node.Timestamp+60 < time.Now().Unix() {

					prop.Node.IsOffline = true
				}

				if err = rows.Err(); err != nil {
					log.Fatal(err)
				}

				err = rows.Close()
				checkError(err)

				if errr := rows.Err(); errr != nil {
					checkError(errr)
				}

				err = stmt.Close()
				checkError(err)

				prevRefresh = time.Now().Unix()

				time.Sleep(10 * time.Millisecond)
			}

			time.Sleep(999 * time.Millisecond)
		}

	}

	fmt.Printf("refresher loop Finished ???\n ")
}
