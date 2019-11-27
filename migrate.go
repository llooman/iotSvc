package main

import (
	"database/sql"
	"fmt"
	"iot"
	"log"
	"strconv"
)

func loadPlansFromDB() {

	var err error

	stmt, errr := database.Prepare(`Select name, id from graphplans where not name is null and not id is null`)
	checkError(errr)

	defer stmt.Close()

	rows, errr := stmt.Query()
	if errr != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	id := 0
	var name string

	for rows.Next() {
		err := rows.Scan(&name, &id)
		checkError(err)

		key := strconv.Itoa(id) + name

		if _, ok := graphPlansMap[key]; !ok {
			graphPlansMap[key] = &GraphPlan{
				Name: name,
				ID:   id,
			}
		}
		fmt.Printf("graphPlan:%#v\n", graphPlansMap[key])
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
}

func persistPlans() {

	// migration from raspbian3
	for _, prp := range props {
		// fmt.Println("k:", k, "v:%v", prp)
		persistPropDef(prp)
		persistPropMem(prp)
	}

	for _, graphPlan := range graphPlansMap {
		// fmt.Println("k:", k, "v:%v", prp)

		persist, err := database2.Prepare(`
		INSERT INTO graphplans( id, 
			name  ) VALUES(?, ? ) 
		ON DUPLICATE KEY UPDATE id=?, 
			name=? `)
		if err != nil {
			fmt.Printf("persistPlans: %v", err.Error())
		}

		defer persist.Close()

		_, err = persist.Exec(
			graphPlan.ID,
			graphPlan.Name,
			graphPlan.ID,
			graphPlan.Name)

		if err != nil {
			fmt.Printf("persistPlans: %v", err.Error())
		}
	}
}

func loadPropValuesFromDB(table string) {

	var err error

	stmt, errr := database.Prepare(fmt.Sprintf(`Select id, ist, istTimer, nextRefresh, error, refreshRate, enabled, retryCount from %s`, table))
	checkError(errr)

	defer stmt.Close()

	rows, errr := stmt.Query()
	if errr != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	varID := 0
	var ist sql.NullInt64
	var istTimer sql.NullInt64
	var nextRefresh sql.NullInt64
	var error sql.NullInt64       // value or isError
	var refreshRate sql.NullInt64 // in use??
	var enabled sql.NullInt64     // in use??
	var retryCount sql.NullInt64

	for rows.Next() {
		err := rows.Scan(&varID, &ist, &istTimer, &nextRefresh, &error, &refreshRate, &enabled, &retryCount)
		checkError(err)

		oldProp := varID % 100
		oldNode := varID / 100

		prop := getProp(oldNode, oldProp)
		// prop := prop(varID)
		iot.SqlInt64(&prop.Val, ist)
		iot.SqlInt64(&prop.ValStamp, istTimer)

		iot.SqlInt(&prop.RefreshRate, refreshRate)
		iot.SqlInt64(&prop.NextRefresh, nextRefresh)
		iot.SqlInt(&prop.RetryCount, retryCount)

		iot.SqlInt64(&prop.Err, error)

		prop.MemDirty = false
		prop.FromDb = true
		prop.IsNew = false
		// SqlInt64(&prop.ErrorTimeStamp, errorTimer)
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
}

func loadPropsFromDB() {

	var err error

	stmt, errr := database.Prepare(`Select id, nodeId, sensId, name, refreshRate, decimalFactor, drawType, drawColor, drawOffset, drawFactor, addOption, trace from iotsensors where nodeId >= 0`)
	checkError(errr)

	defer stmt.Close()

	rows, errr := stmt.Query()
	if errr != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	nodeID := 0
	var name string
	varID := 0
	propID := 0

	var decimalFactor sql.NullInt64
	var refreshRate sql.NullInt64
	var drawType sql.NullInt64
	var drawColor sql.NullString
	var drawOffset sql.NullInt64
	var drawFactor sql.NullFloat64
	var addOption sql.NullInt64
	var trace sql.NullInt64

	for rows.Next() {
		err := rows.Scan(&varID, &nodeID, &propID, &name, &refreshRate, &decimalFactor, &drawType, &drawColor, &drawOffset, &drawFactor, &addOption, &trace)
		checkError(err)

		// varID2 := nodeID * iot.nodeIdVarIdFactor + propID
		prop := getProp(nodeID, propID)
		// prop := prop(varID2)
		prop.Name = name
		iot.SqlInt(&prop.RefreshRate, refreshRate)
		iot.SqlInt(&prop.DrawType, drawType)
		iot.SqlString(&prop.DrawColor, drawColor)
		iot.SqlInt(&prop.DrawOffset, drawOffset)
		iot.SqlFloat32(&prop.DrawFactor, drawFactor)
		if decimalFactor.Valid {
			if decimalFactor.Int64 == 10 {
				prop.Decimals = 1
			} else if decimalFactor.Int64 == 100 {
				prop.Decimals = 2
			} else if decimalFactor.Int64 == 1000 {
				prop.Decimals = 3
			} else {
				prop.Decimals = 0
			}
		}
		//LogOption
		if addOption.Valid {
			if addOption.Int64 == 2 {
				prop.LogOption = 1
			} else if addOption.Int64 == 3 {
				prop.LogOption = 2
			} else {
				prop.LogOption = 0
			}
		}

		if trace.Valid {
			if addOption.Int64 > 0 {
				prop.TraceOption = 1
			} else {
				prop.TraceOption = 0
			}
		}

		// fmt.Printf("prop:%#v", prop)
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
}

func loadNodesFromDB() {

	var err error

	stmt, errr := database.Prepare(`Select nodeId, name from iotnodes where nodeId >= 0`)
	checkError(errr)

	defer stmt.Close()

	rows, errr := stmt.Query()
	if errr != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	var nodeID = 0
	var name string

	for rows.Next() {
		err := rows.Scan(&nodeID, &name)
		checkError(err)

		node := getNode(nodeID)
		node.Name = name
		// fmt.Printf("node:%#v", node)
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
}

func loadNodeValuesFromDB(table string) {

	var err error

	stmt, errr := database.Prepare(fmt.Sprintf(`Select nodeid, connId, boottime, bootcount, timestamp from %s where nodeId >= 0`, table))

	checkError(errr)

	defer stmt.Close()

	rows, errr := stmt.Query()
	if errr != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	nodeID := 0
	connID := 0
	var boottime int64
	bootcount := 0
	var timestamp int64

	for rows.Next() {
		err := rows.Scan(&nodeID, &connID, &boottime, &bootcount, &timestamp)
		checkError(err)

		node := getNode(nodeID)
		node.ConnId = connID
		node.BootCount = bootcount
		node.Timestamp = timestamp
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
}
