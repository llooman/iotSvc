package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"iot"
	"log"
	"math"
	"os"
	"strings"

	// "strconv"
	// "path"
	"database/sql"
	"time"
)

type iotTable struct {
	Name    string
	Columns []iotColumn
	PKey    string
	Engine  string
}

type iotColumn struct {
	Name    string
	ColType string
}

type iotKey struct {
	Name   string
	Create string
}

var optimizeLogMemRunning bool
var dbJson string
var dbCheckSum string

func checkDB() {

	dbCheckSum = "2019-11-02"
	var dbDef []iotTable

	dbJson = `[ 	
		{"Name":"nodesDef",
		"Columns":[ 
			{"Name":"nodeId" ,"ColType":"INT(10) NOT NULL"} 
			,{"Name":"name"  ,"ColType":"VARCHAR(100) DEFAULT NULL"}
			,{"Name":"connId","ColType":"INT(10) NOT NULL"} 
			,{"Name":"nodeType","ColType":"INT(10) NOT NULL"} 
		],
		"PKey":"nodeId",
		"Engine":"InnoDB DEFAULT CHARSET=latin1"}

		,{"Name":"nodesDsk",
		"Columns":[ 
			{"Name":"nodeId"   ,"ColType":"INT(10) NOT NULL"} 
			,{"Name":"bootCount","ColType":"INT(10) NOT NULL DEFAULT 0"} 
			,{"Name":"freeMem"  ,"ColType":"INT(10) NOT NULL DEFAULT 0"} 
			,{"Name":"bootTime" ,"ColType":"BIGINT(20) UNSIGNED DEFAULT NULL"} 
			,{"Name":"timestamp","ColType":"BIGINT(20) UNSIGNED DEFAULT NULL"}
		], 
		"PKey":"nodeId", 
		"Engine":"InnoDB DEFAULT CHARSET=latin1"}

		,{"Name":"nodesMem",
		"Columns":[ 
			{"Name":"nodeId"   ,"ColType":"INT(10) NOT NULL"} 
			,{"Name":"bootCount","ColType":"INT(10) NOT NULL DEFAULT 0"} 
			,{"Name":"freeMem"  ,"ColType":"INT(10) NOT NULL DEFAULT 0"} 
			,{"Name":"bootTime" ,"ColType":"BIGINT(20) UNSIGNED DEFAULT NULL"} 
			,{"Name":"timestamp","ColType":"BIGINT(20) UNSIGNED DEFAULT NULL"}
		], 
		"PKey":"nodeId", 
		"Engine":"MEMORY DEFAULT CHARSET=latin1"}

		,{"Name":"propsDef",
		"Columns":[ 
			{"Name":"nodeId"   ,"ColType":"INT(10) NOT NULL"} 
			,{"Name":"propId"   ,"ColType":"INT(10) NOT NULL"} 
			,{"Name":"name"     ,"ColType":"VARCHAR(100) DEFAULT NULL"} 

			,{"Name":"decimals" 	,"ColType":"INT(10) NOT NULL DEFAULT 0"} 
			,{"Name":"logOption" 	,"ColType":"INT(10) NOT NULL DEFAULT 0"} 
			,{"Name":"traceOption" 	,"ColType":"INT(10) NOT NULL DEFAULT 0"} 
			,{"Name":"localMvc" 	,"ColType":"TINYINT(3) NOT NULL DEFAULT 0"} 
			,{"Name":"globalMvc" 	,"ColType":"TINYINT(3) NOT NULL DEFAULT 0"} 
			,{"Name":"isStr" 		,"ColType":"TINYINT(3) NOT NULL DEFAULT 0"} 
			,{"Name":"onRead" 		,"ColType":"TINYINT(3) NOT NULL DEFAULT 0"} 
			,{"Name":"onSave" 		,"ColType":"TINYINT(3) NOT NULL DEFAULT 0"} 

			,{"Name":"drawType" 	,"ColType":"INT(10) NOT NULL DEFAULT 0"} 
			,{"Name":"drawOffset" 	,"ColType":"INT(10) NOT NULL DEFAULT 0"} 
			,{"Name":"drawColor"    ,"ColType":"VARCHAR(100) NOT NULL DEFAULT 'black'"} 
			,{"Name":"drawFactor" 	,"ColType":"DECIMAL(7,4) NOT NULL DEFAULT 0.0000"} 

			,{"Name":"refreshRate"  ,"ColType":"INT(10) NOT NULL DEFAULT 0"} 
		],
		"PKey":"nodeId,propId", 
		"Engine":"InnoDB DEFAULT CHARSET=latin1"}

		,{"Name":"propsDsk",
		"Columns":[ 
			{"Name":"nodeId"     , "ColType":"INT(10) NOT NULL"} 
			,{"Name":"propId"     , "ColType":"INT(10) NOT NULL"} 
			,{"Name":"val"     	, "ColType":"BIGINT(20) DEFAULT NULL"} 
			,{"Name":"valString", "ColType":"VARCHAR(100) DEFAULT NULL"} 
			,{"Name":"valStamp" , "ColType":"BIGINT(20) UNSIGNED DEFAULT NULL"} 
			,{"Name":"inErr" 	, "ColType":"TINYINT(3) NOT NULL DEFAULT 0"} 
			,{"Name":"err"   	, "ColType":"BIGINT(20) DEFAULT NULL"} 
			,{"Name":"errStamp" , "ColType":"BIGINT(20) UNSIGNED DEFAULT NULL"} 
			,{"Name":"retryCnt"   ,"ColType":"INT(10) NOT NULL"} 
			,{"Name":"nextRefresh", "ColType":"BIGINT(20) UNSIGNED DEFAULT NULL"} 
		],
		"PKey":"nodeId,propId", 
		"Engine":"InnoDB DEFAULT CHARSET=latin1"}

		,{"Name":"propsMem",
		"Columns":[ 
			{"Name":"nodeId"     , "ColType":"INT(10) NOT NULL"} 
			,{"Name":"propId"     , "ColType":"INT(10) NOT NULL"} 
			,{"Name":"val"     	, "ColType":"BIGINT(20) DEFAULT NULL"} 
			,{"Name":"valString", "ColType":"VARCHAR(100) DEFAULT NULL"} 
			,{"Name":"valStamp" , "ColType":"BIGINT(20) UNSIGNED DEFAULT NULL"} 
			,{"Name":"inErr" 	, "ColType":"TINYINT(3) NOT NULL DEFAULT 0"} 
			,{"Name":"err"   	, "ColType":"BIGINT(20) DEFAULT NULL"} 
			,{"Name":"errStamp" , "ColType":"BIGINT(20) UNSIGNED DEFAULT NULL"} 
			,{"Name":"retryCnt"   ,"ColType":"INT(10) NOT NULL"} 
			,{"Name":"nextRefresh", "ColType":"BIGINT(20) UNSIGNED DEFAULT NULL"} 
		],
		"PKey":"nodeId,propId", 
		"Engine":"MEMORY DEFAULT CHARSET=latin1"}

		,{"Name":"logMem",
		"Columns":[ 
			{"Name":"nodeId"     , "ColType":"INT(10) NOT NULL"} 
			,{"Name":"propId"     , "ColType":"INT(10) NOT NULL"} 
			,{"Name":"stamp" , "ColType":"BIGINT(20) UNSIGNED NOT NULL"} 
			,{"Name":"val"   , "ColType":"BIGINT(20) DEFAULT NULL"} 
			,{"Name":"delta" , "ColType":"BIGINT(20) DEFAULT NULL"} 
		],
		"PKey":"nodeId,propId, stamp", 
		"Engine":"MEMORY DEFAULT CHARSET=latin1"}

		,{"Name":"logDsk",
		"Columns":[ 
			{"Name":"nodeId"     , "ColType":"INT(10) NOT NULL"} 
			,{"Name":"propId"     , "ColType":"INT(10) NOT NULL"} 
			,{"Name":"stamp" , "ColType":"BIGINT(20) UNSIGNED NOT NULL"} 
			,{"Name":"val"   , "ColType":"BIGINT(20) DEFAULT NULL"} 
			,{"Name":"delta" , "ColType":"BIGINT(20) DEFAULT NULL"} 
		],
		"PKey":"nodeId,propId, stamp", 
		"Engine":"InnoDB DEFAULT CHARSET=latin1"}

		,{"Name":"verwarming",
		"Columns":[ 
			{"Name":"datum"    , "ColType":"VARCHAR(16) NOT NULL"} 
			,{"Name":"type"    , "ColType":"INT(10) NOT NULL"} 
			,{"Name":"perioden", "ColType":"VARCHAR(256) DEFAULT NULL"}  
		],
		"PKey":"datum", 
		"Engine":"InnoDB DEFAULT CHARSET=latin1"}
		
		,{"Name":"schedules",
		"Columns":[ 
			{"Name":"name"    , 	"ColType":"VARCHAR(100) NOT NULL"} 
			,{"Name":"schedule",	"ColType":"VARCHAR(100) NOT NULL"} 
			,{"Name":"action",  	"ColType":"VARCHAR(100) NOT NULL"}  
			,{"Name":"enabled",		"ColType":"INT(10) NOT NULL"} 
			,{"Name":"execOnStart", "ColType":"TINYINT(3) NOT NULL DEFAULT 0"} 
			,{"Name":"next",	"ColType":"BIGINT(20) UNSIGNED DEFAULT 0"}  
		],
		"PKey":"name", 
		"Engine":"InnoDB DEFAULT CHARSET=latin1"}

		,{"Name":"graphPlans",
		"Columns":[ 
			{"Name":"name"     ,"ColType":"VARCHAR(255) NOT NULL"} 
			,{"Name":"id"       ,"ColType":"VARCHAR(16) NOT NULL"} 
		],
		"PKey":"id,name", 
		"Engine":"InnoDB DEFAULT CHARSET=latin1"}

	]
	`

	err2 := json.Unmarshal([]byte(dbJson), &dbDef)
	if err2 != nil {
		iot.Err.Printf("DB Json.err:%s ", err2.Error())
		os.Exit(1)
	}

	for _, tbl := range dbDef {

		if tableNotExists(tbl.Name) {
			createTable(tbl)

		} else {
			// fmt.Printf("exists %+v  \n", tbl)
			// fmt.Printf("exists %s  \n", tbl.Name)
			for _, col := range tbl.Columns {
				checkColumn(tbl.Name, col)

			}
		}
	}

	refreshViews()
	refreshProcs()

}

func refreshViews() {

	sql := `CREATE OR REPLACE VIEW vwTrace AS 
			SELECT (to_days(now()) - to_days(from_unixtime(timestamp))) AS dd 
			, date_format(from_unixtime(timestamp)
			, '%H:%i:%s') AS at 
			, timestamp AS ts
			, van AS f
			, code AS id
			, val AS v
			, errLevel
			, msg FROM domoTraceMem 
			UNION 
			SELECT (to_days(now()) - to_days(from_unixtime(timestamp))) AS dd 
			, date_format(from_unixtime(timestamp),'%H:%i:%s') AS at 
			, timestamp AS ts
			, van AS f
			, code AS id
			, val AS v
			, errLevel
			, msg FROM domoTrace`

	_, err := iot.DatabaseNew.Query(sql)
	if err != nil {
		iot.Err.Printf("refreshViews: %v", err.Error())
	}
}

func refreshProcs() {

	saveMem()
	logMemDisk()
}
func logMemDisk() {

	sql := "DROP PROCEDURE IF EXISTS logMem2Disk"

	_, err := iot.DatabaseNew.Exec(sql)

	if err != nil {
		iot.Err.Printf("refreshProcs: %v", err.Error())
	}

	sql = `
	CREATE PROCEDURE logMem2Disk ()
		
	BEGIN		
	DECLARE myCount bigint(9); 


	SET @sql1 = 'insert into domo.logdsk SELECT * FROM domo.logmem where not val is null and not exists ( select * from logdsk where logdsk.stamp = logmem.stamp and logdsk.nodeid = logmem.nodeid and logdsk.propid = logmem.propid )'; 
	PREPARE stmt1 FROM @sql1;  EXECUTE stmt1;  DEALLOCATE PREPARE stmt1; 

	SET @sql2 = 'delete from domo.logmem where val is null or exists ( select * from logdsk where logdsk.stamp = logmem.stamp and logdsk.nodeid = logmem.nodeid and logdsk.propid = logmem.propid )'; 
	PREPARE stmt2 FROM @sql2;  EXECUTE stmt2;  DEALLOCATE PREPARE stmt2; 

	END`

	_, err = iot.DatabaseNew.Exec(sql)

	if err != nil {
		iot.Err.Printf("refreshProcs: %v", err.Error())
	}
}

func saveMem() {

	sql := "DROP PROCEDURE IF EXISTS saveMem"

	_, err := iot.DatabaseNew.Query(sql)

	if err != nil {
		iot.Err.Printf("refreshProcs: %v", err.Error())
	}

	sql = `
	CREATE PROCEDURE saveMem ()
		
	BEGIN		
	DECLARE myCount bigint(9); 

	SET @sql0 = 'SELECT count(*)  INTO @myCount from nodesMem'; 
	PREPARE stmt0 FROM @sql0;  EXECUTE stmt0;  DEALLOCATE PREPARE stmt0; 

	IF ( @myCount > 0 ) THEN 
		delete from nodesDsk; 
		insert into nodesDsk select * from nodesMem; 
	ELSE 
		insert into nodesMem select * from nodesDsk; 
	END IF; 

	SET @sql1 = 'SELECT count(*)  INTO @myCount from propsMem'; 
	PREPARE stmt1 FROM @sql1;  EXECUTE stmt1;  DEALLOCATE PREPARE stmt1; 

	IF ( @myCount > 0 ) THEN 
		delete from propsDsk; 
		insert into propsDsk select * from propsMem; 
	ELSE 
		insert into propsMem select * from propsDsk; 
	END IF;

	SET @sql2 = 'insert into domo.logdsk SELECT * FROM domo.logmem where not val is null and not exists ( select * from logdsk where logdsk.stamp = logmem.stamp and logdsk.nodeid = logmem.nodeid and logdsk.propid = logmem.propid )'; 
	PREPARE stmt2 FROM @sql2;  EXECUTE stmt2;  DEALLOCATE PREPARE stmt2; 

	END`

	_, err = iot.DatabaseNew.Query(sql)

	if err != nil {
		iot.Err.Printf("refreshProcs: %v", err.Error())
	}
}

func createTable(table iotTable) {

	sql := fmt.Sprintf("CREATE TABLE %s (", table.Name)

	for _, col := range table.Columns {
		sql += fmt.Sprintf("%s %s,", col.Name, col.ColType)
	}

	sql += fmt.Sprintf(" primary key (%s)) ENGINE=%s;", table.PKey, table.Engine)

	fmt.Println(sql)

	_, err := iot.DatabaseNew.Exec(sql)
	if err != nil {
		iot.Err.Println(err.Error())
		return
	}
}

func checkColumn(table string, column iotColumn) {
	var Field string
	var ColumnType string
	var Null string
	var Key sql.NullString
	var Default sql.NullString
	var Extra sql.NullString

	// var decimalFactor sql.NullInt64

	columnRow, errr := iot.DatabaseNew.Query(fmt.Sprintf("SHOW COLUMNS from %s like '%s'", table, column.Name))
	if errr != nil {
		iot.Err.Printf("checkColumn: %v", errr.Error())
		// log.Fatal(errr)
	}
	defer columnRow.Close()

	if columnRow.Next() {
		// fmt.Printf("exists %+v  \n", tbl)
		columnRow.Scan(&Field, &ColumnType, &Null, &Key, &Default, &Extra)
		//           	Field|  Type       | Null | Key | Default | Extra
		// fmt.Printf("%s: %s %s, null:%s, key:%s, default:%s, extra:%s\n", table, Field, ColumnType, Null, Key.String, Default.String, Extra.String)

		current := ColumnType
		if Null == "NO" {
			if Default.String == "" {
				current += fmt.Sprintf(" NOT NULL")

			} else if strings.HasPrefix(strings.ToUpper(ColumnType), "VARCHAR") {
				current += fmt.Sprintf(" NOT NULL DEFAULT '%s'", Default.String)

			} else {
				current += fmt.Sprintf(" NOT NULL DEFAULT %s", Default.String)
			}
		} else {
			if Default.String == "" {
				current += fmt.Sprintf(" DEFAULT NULL")

			} else {
				current += fmt.Sprintf(" NULL ?? DEFAULT %s", Default.String)

			}
		}

		if column.ColType == "DELETE" {
			iot.Info.Printf("dropColumn %s %s\n", column.Name, column.ColType)
			dropColumn, errr := iot.DatabaseNew.Query(fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", table, column.Name))
			if errr != nil {
				log.Fatal(errr)
			}
			defer dropColumn.Close()

		} else if strings.ToUpper(current) != strings.ToUpper(column.ColType) {
			iot.Info.Printf("modifyColumn %s |%s>%s|\n", column.Name, current, column.ColType)
			modifyColumn, errr := iot.DatabaseNew.Query(fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s", table, column.Name, column.ColType))
			if errr != nil {
				log.Fatal(errr)
			}
			defer modifyColumn.Close()

		}

	} else if column.ColType != "DELETE" {
		iot.Info.Printf("addColumn %s %s\n", column.Name, column.ColType)
		addColumn, errr := iot.DatabaseNew.Query(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column.Name, column.ColType))
		if errr != nil {
			log.Fatal(errr)
		}
		defer addColumn.Close()
	}
}

func tableNotExists(table string) bool {

	tableRow, errr := iot.DatabaseNew.Query(fmt.Sprintf("SHOW TABLES LIKE '%s'", table))
	if errr != nil {
		log.Fatal(errr)
	}
	defer tableRow.Close()

	if tableRow.Next() {
		return false
	}

	return true
}

func optimizeLogMem() {

	optimizeLogMemRunning = true

	nowTimeStamp := time.Now().Unix()

	queryLogMem, errr := iot.DatabaseNew.Prepare(`
	SELECT stamp, nodeId, propId, val, delta FROM logMem
	where nodeId = ? and propId = ? and not val is null and stamp <= ? ORDER BY stamp ASC
	`)

	checkError(errr)
	defer queryLogMem.Close()

	nullifyVal, errrr := iot.DatabaseNew.Prepare(`
	Update logMem set val = null  
	where nodeId = ? and propId = ? and stamp = ?  `)

	checkError(errrr)
	defer nullifyVal.Close()

	var stamp int64
	var nodeid int
	var propid int
	var val int64
	var delta sql.NullInt64

	skipCount := 2 // number of values we skip to decide which can be cleaned
	currCount := 0

	for _, node := range iot.Nodes {
		for _, prop := range node.Props {
			if prop.LogOption > 0 {

				memRows, errr := queryLogMem.Query(prop.NodeId, prop.PropId, nowTimeStamp) // nowTimeStamp

				checkError(errr)

				defer memRows.Close()

				currCount = 0

				var timestamps [3]int64
				var values [3]int64
				var deltas [3]int64

				skipMiddleValue := false
				//fmt.Printf("next nodeid:%d propid:%d  LogOption:%d \n", prop.NodeId, prop.PropId, prop.LogOption)

				for memRows.Next() {

					err := memRows.Scan(&stamp, &nodeid, &propid, &val, &delta)

					checkError(err)

					//fmt.Printf("next val:%d stamp:%d \n", val, stamp)

					// keep history of 3 values in a row: 0=oldest, 2=most recent
					values[0] = values[1]
					values[1] = values[2]
					values[2] = val
					timestamps[0] = timestamps[1]
					timestamps[1] = timestamps[2]
					timestamps[2] = stamp
					deltas[0] = deltas[1]
					deltas[1] = deltas[2]
					iot.SqlInt64(&deltas[2], delta)

					if currCount <= skipCount {
						currCount++

					} else {
						// LogOption 0 = no logging, 1 = skip when equal, 2 skip extraPolate
						skipMiddleValue = false

						// always skip this value when there are 3 equal values (EqualWithPrevious || extrapolate)
						if prop.LogOption == 1 || prop.LogOption == 2 {

							if values[0] == values[1] && values[1] == values[2] {
								skipMiddleValue = true
								iot.Trace.Printf("skip all 3 equal val:%d stamp:%d\n", values[1], timestamps[1])
							}
						}

						// extrapolate 3 in a row values: When 3 comes close then skip 2=the middle one
						if prop.LogOption == 2 && !skipMiddleValue {

							partialTimeSpan := float64(timestamps[1] - timestamps[0])
							totalTimeSpan := float64(timestamps[2] - timestamps[0])
							partialDelta := float64(values[1] - values[0])
							totalDelta := float64(values[2] - values[0])
							calculatedDelta := partialDelta * totalTimeSpan / partialTimeSpan
							//fmt.Printf("extrapolate partialDelta:%f totalDelta:%f calculatedDelta:%f\n", partialDelta, totalDelta, calculatedDelta)
							if math.Abs(totalDelta-calculatedDelta) < 0.6 {
								skipMiddleValue = true
								iot.Trace.Printf("skip extrapolate val:%d stamp:%d\n", values[1], timestamps[1])
							}
						}

						if skipMiddleValue == true {
							// fmt.Printf("skipMiddleValue nodeId:%d propId:%d stamp:%d \n", prop.NodeId, prop.PropId, timestamps[1])

							_, errrrr := nullifyVal.Exec(prop.NodeId, prop.PropId, timestamps[1]) // nowTimeStamp

							checkError(errrrr)
							defer nullifyVal.Close()

							// replace middel by oldest so next iteration
							values[1] = values[0]
							timestamps[1] = timestamps[0]
							deltas[1] = deltas[0]
						}
					}
				}
			}
		}
	}

	optimizeLogMemRunning = false
}

func timedata2(respBuff *bytes.Buffer, iotPayload *iot.IotPayload) {

	var err error

	// fmt.Printf("timedata2: %v\n", iotPayload)

	/*
	*  combine all values from domoLog and domoLogMem
	*  find 1 more values just over the edge.
	*  ! duplicates are automatically removed by the Distinct
	 */
	stmt, errr := iot.DatabaseNew.Prepare(`
	Select Distinct * from
	(  	  ( SELECT stamp, val from logDsk WHERE nodeid = ? and propid = ? and not val is null and stamp <= ?               ORDER BY stamp DESC LIMIT 0,1 )
	Union ( SELECT stamp, val from logDsk WHERE nodeid = ? and propid = ? and not val is null and stamp > ? and stamp <= ? ORDER BY stamp )
	Union ( SELECT stamp, val from logMem WHERE nodeid = ? and propid = ? and not val is null and stamp <= ?               ORDER BY stamp DESC LIMIT 0,1 )
	Union ( SELECT stamp, val from logMem WHERE nodeid = ? and propid = ? and not val is null and stamp > ? and stamp <= ? ORDER BY stamp )
	) as result ORDER BY stamp`)

	checkError(errr)

	defer stmt.Close()

	nodeId := iotPayload.NodeId
	propId := iotPayload.PropId

	rows, errr := stmt.Query(
		nodeId, propId, iotPayload.Start,
		nodeId, propId, iotPayload.Start, iotPayload.Stop,
		nodeId, propId, iotPayload.Start,
		nodeId, propId, iotPayload.Start, iotPayload.Stop)
	if errr != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	var startTimestamp = 0
	var timestamp = 0
	var val = 0
	var prevTimestamp = 0
	var cnt = 0

	respBuff.Write([]byte(fmt.Sprintf(`{"id":"n%dp%d","from":%d,"timedata":[`, nodeId, propId, iotPayload.Start)))
	// fmt.Printf(`{"id":%d,"from":%d\n`, req.Id, req.Start)

	for rows.Next() {
		err := rows.Scan(&timestamp, &val)
		checkError(err)

		cnt++

		if cnt == 1 {
			startTimestamp = timestamp
		}

		var deltaTijd = timestamp - prevTimestamp

		if cnt > 1 {
			respBuff.Write([]byte(`,`))
		}
		respBuff.Write([]byte(fmt.Sprintf("%d,%d", deltaTijd, val)))
		if cnt > 825 {
			// fmt.Printf("[%d,%d,%d]\n", cnt, prevTimestamp, timestamp)
			// fmt.Printf("[%d,%d,%d]\n", cnt, deltaTijd, deltaVal)
		}

		prevTimestamp = timestamp
	}

	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}

	respBuff.Write([]byte(fmt.Sprintf(`],"count":%d,"start":%d,"stop":%d,"stopval":%d}`, cnt, startTimestamp, timestamp, val)))

	// fmt.Printf(`timedata: ],"count":%d,"start":%d,"stop":%d,"stopval":%d}\n\n`, cnt, startTimestamp, timestamp, val)

	respBuff.Write([]byte("\n"))

	// fmt.Printf("%s\n", respBuff.String())

	err = rows.Close()
	checkError(err)

	if errr := rows.Err(); errr != nil {
		checkError(errr)
	}

	err = stmt.Close()
	checkError(err)
}

func timedataDeltas2(respBuff *bytes.Buffer, iotPayload *iot.IotPayload) {

	var err error

	/* return delta i.p.v. val
	*  combine all values from domoLog and domoLogMem
	*  find 1 more values just over the edge.
	*  ! duplicates are automatically removed by the Distinct
	 */
	stmt, errr := iot.DatabaseNew.Prepare(`
	Select Distinct * from
	(  	  ( SELECT stamp, delta from logDsk    WHERE nodeid = ? and propid = ? and  delta > 0 and stamp <= ?               ORDER BY stamp DESC LIMIT 0,1 )
	Union ( SELECT stamp, delta from logDsk    WHERE nodeid = ? and propid = ? and  delta > 0 and stamp > ? and stamp <= ? ORDER BY stamp )
	Union ( SELECT stamp, delta from logMem WHERE nodeid = ? and propid = ? and  delta > 0 and stamp <= ?               ORDER BY stamp DESC LIMIT 0,1 )
	Union ( SELECT stamp, delta from logMem WHERE nodeid = ? and propid = ? and  delta > 0 and stamp > ? and stamp <= ? ORDER BY stamp )
	) as result ORDER BY stamp`)

	checkError(errr)

	defer stmt.Close()

	nodeId := iotPayload.NodeId
	propId := iotPayload.PropId

	rows, errr := stmt.Query(
		nodeId, propId, iotPayload.Start,
		nodeId, propId, iotPayload.Start, iotPayload.Stop,
		nodeId, propId, iotPayload.Start,
		nodeId, propId, iotPayload.Start, iotPayload.Stop)
	if errr != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	var startTimestamp = 0
	var timestamp = 0
	var delta = 0
	var prevTimestamp = 0
	var cnt = 0

	respBuff.Write([]byte(fmt.Sprintf(`{"id":"n%dp%d","from":%d,"timedata":[`, nodeId, propId, iotPayload.Start)))
	// fmt.Printf(`{"id":%d,"from":%d\n`, req.Id, req.Start)

	for rows.Next() {
		err := rows.Scan(&timestamp, &delta)
		checkError(err)

		cnt++

		if cnt == 1 {
			startTimestamp = timestamp
		}

		var deltaTijd = timestamp - prevTimestamp

		if cnt > 1 {
			respBuff.Write([]byte(`,`))
		}
		respBuff.Write([]byte(fmt.Sprintf("%d,%d", deltaTijd, delta)))
		if cnt > 825 {
			// fmt.Printf("[%d,%d,%d]\n", cnt, prevTimestamp, timestamp)
			// fmt.Printf("[%d,%d,%d]\n", cnt, deltaTijd, deltaVal)
		}

		prevTimestamp = timestamp
	}

	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}

	respBuff.Write([]byte(fmt.Sprintf(`],"count":%d,"start":%d,"stop":%d,"stopdelta":%d}`, cnt, startTimestamp, timestamp, delta)))

	// fmt.Printf(`timedata: ],"count":%d,"start":%d,"stop":%d,"stopval":%d}\n\n`, cnt, startTimestamp, timestamp, delta)

	respBuff.Write([]byte("\n"))
	// fmt.Printf("%s\n", respBuff.String())

	err = rows.Close()
	checkError(err)

	if errr := rows.Err(); errr != nil {
		checkError(errr)
	}

	err = stmt.Close()
	checkError(err)
}

func timedataDiff2(respBuff *bytes.Buffer, iotPayload *iot.IotPayload) {

	var err error

	//fmt.Printf("timedataDiff2 id:%s start:%d stop:%d\n", iotPayload.Id, iotPayload.Start, iotPayload.Stop)

	/*
	*  combine all values from domoLog and domoLogMem
	*  find 1 more values just over the edge.
	*  ! duplicates are automatically removed by the Distinct
	 */
	stmt, errr := iot.DatabaseNew.Prepare(`
	Select Distinct * from
	(  	  ( SELECT stamp, val from logDsk WHERE nodeid = ? and propid = ? and not val is null and stamp <= ?               ORDER BY stamp DESC LIMIT 0,1 )
	Union ( SELECT stamp, val from logDsk WHERE nodeid = ? and propid = ? and not val is null and stamp > ? and stamp <= ? ORDER BY stamp )
	Union ( SELECT stamp, val from logMem WHERE nodeid = ? and propid = ? and not val is null and stamp <= ?               ORDER BY stamp DESC LIMIT 0,1 )
	Union ( SELECT stamp, val from logMem WHERE nodeid = ? and propid = ? and not val is null and stamp > ? and stamp <= ? ORDER BY stamp )
	) as result ORDER BY stamp`)

	checkError(errr)

	defer stmt.Close()

	nodeId := iotPayload.NodeId
	propId := iotPayload.PropId

	rows, errr := stmt.Query(
		nodeId, propId, iotPayload.Start,
		nodeId, propId, iotPayload.Start, iotPayload.Stop,
		nodeId, propId, iotPayload.Start,
		nodeId, propId, iotPayload.Start, iotPayload.Stop)
	if errr != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	var startTimestamp = 0
	var timestamp = 0
	var val = 0
	var prevTimestamp = 0
	var prevVal = 0
	var cnt = 0

	respBuff.Write([]byte(fmt.Sprintf(`{"id":"n%dp%d","from":%d,"timedata":[`, nodeId, propId, iotPayload.Start)))

	for rows.Next() {
		err := rows.Scan(&timestamp, &val)
		checkError(err)

		cnt++

		if cnt == 1 {
			startTimestamp = timestamp
		}

		var deltaVal = val - prevVal
		var deltaTijd = timestamp - prevTimestamp
		if deltaVal != 0 {

			if cnt > 1 {
				respBuff.Write([]byte(`,`))
			}
			respBuff.Write([]byte(fmt.Sprintf("%d,%d", deltaTijd, deltaVal)))
			if cnt > 825 {
				// fmt.Printf("[%d,%d,%d]\n", cnt, prevTimestamp, timestamp)
				// fmt.Printf("[%d,%d,%d]\n", cnt, deltaTijd, deltaVal)
			}

			prevTimestamp = timestamp
			prevVal = val
		}
	}

	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}

	respBuff.Write([]byte(fmt.Sprintf(`],"count":%d,"start":%d,"stop":%d,"stopval":%d}`, cnt, startTimestamp, timestamp, val)))

	// fmt.Printf(`timedata: ],"count":%d,"start":%d,"stop":%d,"stopval":%d}\n\n`, cnt, startTimestamp, timestamp, val)

	respBuff.Write([]byte("\n"))

	// fmt.Printf("%s\n", respBuff.String())

	err = rows.Close()
	checkError(err)

	if errr := rows.Err(); errr != nil {
		checkError(errr)
	}

	err = stmt.Close()
	checkError(err)
}

func savePlans(respBuff *bytes.Buffer, iotPayload *iot.IotPayload) {

	var err error
	name := iotPayload.Parms[1]
	ids := iotPayload.Parms[2]

	_, err = iot.DatabaseNew.Query(fmt.Sprintf(`delete from graphPlans where name = '%s'`, name))
	checkError(err)

	if ids != "" {

		idsArr := strings.Split(ids, "_")

		for _, id := range idsArr {

			sql := fmt.Sprintf(`insert into graphPlans (id, name) values ('%s','%s')`, id, name)
			// Trace.Printf(sql)

			_, err = iot.DatabaseNew.Query(sql)
			checkError(err)

		}
	}
	respBuff.Write([]byte(`{"retCode":0}`))

	// {\"retCode\":0,\"message\":\"\"}
}

func graphPlans(respBuff *bytes.Buffer) {

	var err error

	stmt, errr := iot.DatabaseNew.Prepare("Select id, name from graphPlans  order by name, id")

	checkError(errr)

	defer stmt.Close()

	rows, errr := stmt.Query()
	if errr != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	respBuff.Write([]byte(fmt.Sprintf(`{"graphPlans":[`)))

	cnt := 0

	var id string
	var nextName string
	currName := ""

	var ids bytes.Buffer

	for rows.Next() {
		err := rows.Scan(&id, &nextName)
		checkError(err)

		cnt++

		if currName == "" ||
			currName != nextName {

			if currName != "" {
				respBuff.Write([]byte(fmt.Sprintf("%s]}", ids.String())))
			}
			if cnt > 1 {
				respBuff.Write([]byte(","))
			}

			respBuff.Write([]byte(fmt.Sprintf(`{"graphPlan":"%s","ids":[`, nextName)))

			ids.Reset()
			// ids.Write([]byte(id))
			ids.Write([]byte(fmt.Sprintf(`"%s"`, id)))
		} else {
			ids.Write([]byte(fmt.Sprintf(`,"%s"`, id)))
		}
		currName = nextName
	}

	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}

	if respBuff.Len() > 0 {
		respBuff.Write([]byte(fmt.Sprintf(`%s]}`, ids.String())))
	}

	if cnt > 0 {
		respBuff.Write([]byte("]}"))
	}
	respBuff.Write([]byte("\n"))
	// fmt.Printf("%s\n", respBuff.String())

	err = stmt.Close()
	checkError(err)
}

func graphs(respBuff *bytes.Buffer) {

	var err error

	stmt, errr := iot.DatabaseNew.Prepare(`
	  Select id, name, drawType, drawColor, drawOffset, drawFactor
	  from IoTSensors where drawType > 0 order by id`)

	checkError(errr)

	defer stmt.Close()

	rows, errr := stmt.Query()
	if errr != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	//{"graphs":[{"id":113,"name":"connMem","factor":0.020,"type":1,"color":"black"},{"id":213,
	respBuff.Write([]byte(fmt.Sprintf(`{"graphPlan":"Pomp","graphs":[`)))
	// fmt.Printf(`{"id":%d,"from":%d\n`, req.Id, req.Start)

	cnt := 0

	var id int
	var name string
	var drawtype int64
	var drawtypeSql sql.NullInt64
	var color string
	var drawOffset float64
	var drawFactor float64
	var drawOffsetSql sql.NullFloat64
	var drawFactorSql sql.NullFloat64

	for rows.Next() {
		err := rows.Scan(&id, &name, &drawtypeSql, &color, &drawOffsetSql, &drawFactorSql)
		checkError(err)

		cnt++

		if cnt == 1 {
			// startTimestamp = timestamp
		}

		if cnt > 1 {
			respBuff.Write([]byte(`,`))
		}

		if drawtype = drawtypeSql.Int64; !drawtypeSql.Valid {
			drawtype = 0
		}

		if drawOffset = drawOffsetSql.Float64; !drawOffsetSql.Valid {
			drawOffset = 0
		}

		if drawFactor = drawFactorSql.Float64; !drawFactorSql.Valid {
			drawFactor = 0
		}

		respBuff.Write([]byte(fmt.Sprintf(`{"id":%d,"name":"%s","type":%d,"color":"%s","drawOffset":%f,"drawFactor":%f}`, id, name, drawtype, color, drawOffset, drawFactor)))

	}

	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}

	// conn.Write([]byte(fmt.Sprintf(`],"count":%d,"start":%d,"stop":%d,"stopval":%d}`, cnt, startTimestamp, timestamp, val)))

	// fmt.Printf(`timedata: ],"count":%d,"start":%d,"stop":%d,"stopval":%d}\n\n`, cnt, startTimestamp, timestamp, val)

	respBuff.Write([]byte("]}\n"))

	err = rows.Close()
	checkError(err)

	if errr := rows.Err(); errr != nil {
		checkError(errr)
	}

	err = stmt.Close()
	checkError(err)
}

func graphs2(respBuff *bytes.Buffer) {

	var err error

	stmt, errr := iot.DatabaseNew.Prepare(`
	  Select nodeId, propId, name, drawType, drawColor, drawOffset, drawFactor
	  from propsdef where drawType > 0 order by  nodeId, propId`)

	checkError(errr)

	defer stmt.Close()

	rows, errr := stmt.Query()
	if errr != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	//{"graphs":[{"id":113,"name":"connMem","factor":0.020,"type":1,"color":"black"},{"id":213,
	respBuff.Write([]byte(fmt.Sprintf(`{"graphPlan":"Pomp","graphs":[`)))

	cnt := 0

	var nodeid int
	var propid int
	var name string
	var drawtype int64
	var drawtypeSql sql.NullInt64
	var color string
	var drawOffset float64
	var drawFactor float64
	var drawOffsetSql sql.NullFloat64
	var drawFactorSql sql.NullFloat64

	for rows.Next() {
		err := rows.Scan(&nodeid, &propid, &name, &drawtypeSql, &color, &drawOffsetSql, &drawFactorSql)
		checkError(err)

		cnt++

		if cnt == 1 {
			// startTimestamp = timestamp
		}

		if cnt > 1 {
			respBuff.Write([]byte(`,`))
		}

		if drawtype = drawtypeSql.Int64; !drawtypeSql.Valid {
			drawtype = 0
		}

		if drawOffset = drawOffsetSql.Float64; !drawOffsetSql.Valid {
			drawOffset = 0
		}

		if drawFactor = drawFactorSql.Float64; !drawFactorSql.Valid {
			drawFactor = 0
		}

		// id := fmt.Sprintf("n%dp%d", nodeid, propid )
		respBuff.Write([]byte(fmt.Sprintf(`{"id":"n%dp%d","name":"%s","type":%d,"color":"%s","drawOffset":%f,"drawFactor":%f}`, nodeid, propid, name, drawtype, color, drawOffset, drawFactor)))

	}

	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}

	respBuff.Write([]byte("]}\n"))

	// fmt.Printf("%s\n", respBuff.String())

	err = rows.Close()
	checkError(err)

	if errr := rows.Err(); errr != nil {
		checkError(errr)
	}

	err = stmt.Close()
	checkError(err)
}
