/*

	{"cmd":"mvcdata","type":"global"}
	{"cmd":"timedata"}
	{"cmd":"timedataDiff"}

	0.0.0.0 tcp4 = IPv4 wildcard address

	https://en.wikipedia.org/wiki/Time_series
	dataPoints
	timePoints
*/

package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"strconv"

	// "path"
	"bufio"
	"runtime"
	"strings"
	"time"

	"github.com/tkanos/gonfig"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	_ "github.com/go-sql-driver/mysql"

	"iot"
)

var props map[int]*iot.IotProp
var nodes map[int]*iot.IotNode

var wwwRoot string

// var iotSvc string

var database *sql.DB
var database2 *sql.DB

var pomp *iot.IotNode
var dimmer *iot.IotNode
var ketel *iot.IotNode
var p1 *iot.IotNode
var closeIn *iot.IotNode
var serial1 *iot.IotNode
var serial2 *iot.IotNode
var chint *iot.IotNode
var test *iot.IotNode
var afzuiging *iot.IotNode

type IotConfig struct {
	Port      string
	MqttUp    string
	MqttCmd   string
	MqttDown  string
	MqttUri   string
	Database  string
	Database2 string
	IotSvcOld string
	MqttUri3b string
}

var raspbian3 iot.IotConn
var iotConfig IotConfig
var mqttClient mqtt.Client
var mqttClient3b mqtt.Client

func init() {

}

func main() {

	err := gonfig.GetConf(iot.Config(), &iotConfig)
	if err != nil {
		fmt.Printf("Config %s not found\n", iot.Config())
	}

	// fmt.Printf("MqttCmd:%s \n", iotConfig.MqttCmd)

	nodes = make(map[int]*iot.IotNode)
	props = make(map[int]*iot.IotProp)

	database, err = sql.Open("mysql", iotConfig.Database)
	checkError(err)
	defer database.Close()

	database2, err = sql.Open("mysql", iotConfig.Database2)
	checkError(err)
	defer database2.Close()

	pomp = getNode(3)
	dimmer = getNode(4)
	ketel = getNode(5)
	p1 = getNode(7)
	closeIn = getNode(8)
	serial1 = getNode(11)
	serial2 = getNode(12)
	chint = getNode(14)
	test = getNode(15)
	afzuiging = getNode(23)

	pomp.Timestamp = time.Now().Unix()
	dimmer.Timestamp = time.Now().Unix()
	ketel.Timestamp = time.Now().Unix()
	p1.Timestamp = time.Now().Unix()
	closeIn.Timestamp = time.Now().Unix()
	serial1.Timestamp = time.Now().Unix()
	serial2.Timestamp = time.Now().Unix()
	chint.Timestamp = time.Now().Unix()
	test.Timestamp = time.Now().Unix()
	afzuiging.Timestamp = time.Now().Unix()

	prop := prop(370)

	prop.DefDirty = true
	prop.IsNew = false
	prop.ValString = "xxxx"
	prop.ValStamp = time.Now().Unix()

	loadNodesFromDB()
	loadNodeValuesFromDB("iotnodesdisk")
	loadNodeValuesFromDB("iotnodesmem")

	loadPropsFromDB()
	loadPropValuesFromDB("iotvaluesdisk")
	loadPropValuesFromDB("iotvaluesmem")

	// migration from raspbian3
	for _, prp := range props {
		// fmt.Println("k:", k, "v:%v", prp)
		persistPropDef(prp)
		persistPropMem(prp)
	}

	for _, nod := range nodes {
		// fmt.Println("k:", k, "v:", v)
		persistNodeDef(nod)
		persistNodeMem(nod)
	}

	mqttClient = iot.NewMQClient(iotConfig.MqttUri, "iotSvc")
	mqttClient3b = iot.NewMQClient(iotConfig.MqttUri3b, "iotSvc")

	raspbian3 = iot.IotConn{
		Service:        iotConfig.IotSvcOld,
		Protocol:       "tcp",
		ConnectTimeout: 3,
		ReadTimeout:    3,
		PingInterval:   21,
		Silent:         true,
	}

	// _, errr = TestConn(iotSvcConn, "")
	// checkError(errr)

	go mqListenUp(iotConfig.MqttUp)
	go mqListenCmd(iotConfig.MqttCmd)

	if runtime.GOOS == "windows" {
		err = startIotService("localhost:" + iotConfig.Port)
	} else {
		err = startIotService("0.0.0.0:" + iotConfig.Port)
	}
	checkError(err)

	reader := bufio.NewReader(os.Stdin)
	// scanner := bufio.NewScanner(os.Stdin)

	fmt.Printf("in Service mode\n")
	// fmt.Print("iot: ")

	for true {

		// os.Stdin.SetReadDeadline(time.Now().Add(7 * time.Second)) // 49

		text, _, errr := reader.ReadLine()

		// time.Sleep(3 * time.Second) //49
		// fmt.Print(time.Now())
		// fmt.Println(": " + text)

		if errr != nil {

			err = database.Ping()

			fmt.Print(time.Now())
			fmt.Println(" ping")

			// checkError(err)
			if err != nil {
				fmt.Printf("database.Ping() err: " + err.Error())
				fmt.Fprintf(os.Stderr, "iot: Fatal error: %s", err.Error())
				os.Exit(1)
			}

			err = database2.Ping()

			fmt.Print(time.Now())
			fmt.Println(" ping2")

			// checkError(err)
			if err != nil {
				fmt.Printf("database2.Ping() err: " + err.Error())
				fmt.Fprintf(os.Stderr, "iot: Fatal error: %s", err.Error())
				os.Exit(1)
			}

		} else {

			text = text

			// resp, _ := iotCommand(myClient, "mvcdata&localNode&3\n")
			// fmt.Printf("respons: %s", resp)

			// resp = resp
			// for scanner.Scan() {
			// 	fmt.Print(time.Now())
			// 	fmt.Println(scanner.Text())
			// }

			for k, v := range props {
				fmt.Println("k:", k, "v:", v)
			}

			// for k, v := range nodes {
			// 	fmt.Println("k:", k, "v:", v)
			// }

			fmt.Print("\niot: ")
		}
	}

	fmt.Printf("webstop\n")
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

func getProp(nodeID int, propID int) *iot.IotProp {

	return prop(nodeID*iot.IDFactor + propID)
}

func prop(varID int) *iot.IotProp {

	//fmt.Printf("prop %d\n", id)

	if prop, ok := props[varID]; ok {
		return prop

	} else {
		nodeID := varID / iot.IDFactor

		props[varID] = &iot.IotProp{
			NodeId:    nodeID,
			PropId:    varID % iot.IDFactor,
			VarId:     varID,
			Node:      getNode(nodeID),
			Decimals:  0,
			Name:      "prop" + strconv.Itoa(varID),
			LocalMvc:  false,
			GlobalMvc: false,
			FromDb:    false,
			IsNew:     true,
			DefDirty:  true}

		return props[varID]
	}
}

func createProp(node *iot.IotNode, propID int, name string, decimals int, refreshRate int, logOpt int, traceOpt int, globalMvc bool) *iot.IotProp {

	varID := propID + node.NodeId*iot.IDFactor

	props[varID] = &iot.IotProp{
		NodeId:      node.NodeId,
		PropId:      propID,
		VarId:       varID,
		Node:        node,
		Decimals:    decimals,
		RefreshRate: refreshRate,
		LogOption:   logOpt,
		TraceOption: traceOpt,
		LocalMvc:    true,
		GlobalMvc:   globalMvc,
		Name:        name,
		FromDb:      false,
		IsNew:       true,
		MemDirty:    true,
		DefDirty:    true}

	if refreshRate > 0 {
		props[varID].NextRefresh = time.Now().Unix() + int64(refreshRate)
	}

	return props[varID]
}

func createTemp(node *iot.IotNode, propID int, name string, logOpt int, globalMvc bool) *iot.IotProp {
	prop := createProp(node, propID, name, 2, 60, logOpt, 0, globalMvc)
	prop.LogOption = 2
	return prop
}

func getNode(nodeID int) *iot.IotNode {

	//fmt.Printf("getNode %d\n", nodeID)

	if node, ok := nodes[nodeID]; ok {
		return node

	} else if nodeID == 2 {
		nodes[nodeID] = &iot.IotNode{
			NodeId: nodeID,
			Name:   "iot"}

		createProp(nodes[nodeID], 20, "thuis", 0, 3600, 0, 0, true)
		createProp(nodes[nodeID], 21, "vakantie", 0, 3600, 0, 0, true)

		// pPomp := createProp(nodes[nodeID], 13, "pomp", 0, 60, 1, 0, true)
		// createTemp(nodes[nodeID], 20, "aanvoer", 2)
		// createTemp(nodes[nodeID], 11, "retour", 2)
		// createTemp(nodes[nodeID], 12, "ruimte", 2)
		// createTemp(nodes[nodeID], 14, "muurin", 2)
		// createTemp(nodes[nodeID], 15, "afvoer", 2)
		// createTemp(nodes[nodeID], 16, "keuken", 2)

		// createProp(nodes[nodeID], 52, "refTemp", 1, 3600, 0, 0, true)
		// createProp(nodes[nodeID], 53, "deltaTemp", 1, 3600, 0, 0, true)
		// createProp(nodes[nodeID], 54, "duty", 0, 3600, 0, 0, true)
		// createProp(nodes[nodeID], 55, "dutymin", 0, 3600, 0, 0, true)

		return nodes[nodeID]

	} else if nodeID == 3 {
		nodes[nodeID] = &iot.IotNode{
			NodeId: nodeID,
			Name:   "pomp"}

		createProp(nodes[nodeID], 13, "pomp", 0, 60, 1, 0, true)
		createTemp(nodes[nodeID], 20, "aanvoer", 2, true)
		createTemp(nodes[nodeID], 11, "retour", 2, true)
		createTemp(nodes[nodeID], 12, "ruimte", 2, true)
		createTemp(nodes[nodeID], 14, "muurin", 2, true)
		createTemp(nodes[nodeID], 15, "afvoer", 2, true)
		createTemp(nodes[nodeID], 16, "keuken", 2, true)

		createProp(nodes[nodeID], 52, "refTemp", 1, 3600, 0, 0, true)
		createProp(nodes[nodeID], 53, "deltaTemp", 1, 3600, 0, 0, true)
		createProp(nodes[nodeID], 54, "duty", 0, 3600, 0, 0, true)
		createProp(nodes[nodeID], 55, "dutymin", 0, 3600, 0, 0, true)

		createProp(nodes[nodeID], 70, "testStr", -1, 3600, 0, 0, true)

		return nodes[nodeID]

	} else if nodeID == 4 {
		nodes[nodeID] = &iot.IotNode{
			NodeId: nodeID,
			Name:   "dimmer"}

		// dimmer.publishTopic = "main";
		// dimmer.publishGlobal = true;

		createProp(nodes[nodeID], 52, "dimmer", 0, 60, 0, 0, true)
		createProp(nodes[nodeID], 53, "defLevel", 0, 3600, 0, 0, true)
		createProp(nodes[nodeID], 11, "vin", 1, 60, 0, 0, true)

		return nodes[nodeID]

	} else if nodeID == 5 {
		nodes[nodeID] = &iot.IotNode{
			NodeId: nodeID,
			Name:   "ketel"}

		createProp(nodes[nodeID], 52, "thermostaat", 0, 60, 1, 0, true)
		createProp(nodes[nodeID], 53, "thermostaatPeriode", 0, 3600, 0, 0, true)
		createTemp(nodes[nodeID], 41, "CVTemp", 2, true)
		createTemp(nodes[nodeID], 24, "RetBad", 2, false)
		createTemp(nodes[nodeID], 25, "RetRechts", 2, false)
		createTemp(nodes[nodeID], 26, "RetLinks", 2, false)

		return nodes[nodeID]

	} else if nodeID == 6 {
		nodes[nodeID] = &iot.IotNode{
			NodeId: nodeID,
			Name:   "gas"}

		// createProp(nodes[nodeID], 52, "dimmer", 0, 60, 0, 0, true)
		// createProp(nodes[nodeID], 53, "defLevel", 0, 3600, 0, 0, true)
		// createProp(nodes[nodeID], 11, "vin", 1, 60, 0, 0, true)

		return nodes[nodeID]

	} else if nodeID == 7 {
		nodes[nodeID] = &iot.IotNode{
			NodeId: nodeID,
			Name:   "p1"}

		createProp(nodes[nodeID], 9, "IP", 0, 60, 0, 0, true)       //  LogLong s9.publishGlobal = false;
		createProp(nodes[nodeID], 13, "button", 0, 60, 0, 0, true)  //  Bool s13.publishGlobal = false;
		createProp(nodes[nodeID], 20, "version", 0, 60, 0, 0, true) //  LogLong s9.publishGlobal = false;

		createProp(nodes[nodeID], 21, "vbLaag", 3, 60, 1, 0, true)    //  LogLong s9.publishGlobal = false;
		createProp(nodes[nodeID], 51, "deltaLaag", 3, 60, 0, 0, true) //  LogLong s9.publishGlobal = false;
		createProp(nodes[nodeID], 22, "vbHoog", 3, 60, 1, 0, true)    //  LogLong s9.publishGlobal = false;
		createProp(nodes[nodeID], 52, "deltaHoog", 3, 60, 0, 0, true) //  LogLong s9.publishGlobal = false;

		createProp(nodes[nodeID], 23, "retLaag", 3, 60, 0, 0, true) //  LogLong s9.publishGlobal = false;
		createProp(nodes[nodeID], 24, "retHoog", 3, 60, 0, 0, true) //  LogLong s9.publishGlobal = false;

		createProp(nodes[nodeID], 25, "vbGas", 3, 60, 1, 0, true) //  LogLong s9.publishGlobal = false;

		createProp(nodes[nodeID], 26, "tarief", 0, 60, 0, 0, true) //  LogLong s9.publishGlobal = false;

		createProp(nodes[nodeID], 29, "L1Pow", 3, 60, 0, 0, true)    //  LogLong s9.publishGlobal = false;
		createProp(nodes[nodeID], 30, "L1PowNeg", 3, 60, 0, 0, true) //  LogLong s9.publishGlobal = false;
		createProp(nodes[nodeID], 31, "L1Curr", 0, 60, 0, 0, true)   //  LogLong s9.publishGlobal = false;
		createProp(nodes[nodeID], 32, "L1Volt", 1, 60, 0, 0, true)   //  LogLong s9.publishGlobal = false;

		createProp(nodes[nodeID], 33, "L2Pow", 3, 60, 0, 0, true)    //  LogLong s9.publishGlobal = false;
		createProp(nodes[nodeID], 34, "L2PowNeg", 3, 60, 0, 0, true) //  LogLong s9.publishGlobal = false;
		createProp(nodes[nodeID], 35, "L2Curr", 0, 60, 0, 0, true)   //  LogLong s9.publishGlobal = false;
		createProp(nodes[nodeID], 36, "L2Volt", 1, 60, 0, 0, true)   //  LogLong s9.publishGlobal = false;

		createProp(nodes[nodeID], 37, "L3Pow", 3, 60, 0, 0, true)    //  LogLong s9.publishGlobal = false;
		createProp(nodes[nodeID], 38, "L3PowNeg", 3, 60, 0, 0, true) //  LogLong s9.publishGlobal = false;
		createProp(nodes[nodeID], 39, "L3Curr", 0, 60, 0, 0, true)   //  LogLong s9.publishGlobal = false;
		createProp(nodes[nodeID], 40, "L3Volt", 1, 60, 0, 0, true)   //  LogLong s9.publishGlobal = false;

		return nodes[nodeID]

	} else if nodeID == 8 {
		nodes[nodeID] = &iot.IotNode{
			NodeId: nodeID,
			Name:   "closeIn"}

		createProp(nodes[nodeID], 55, "ssrPower", 0, 60, 0, 0, true)
		createProp(nodes[nodeID], 52, "maxTemp", 0, 3600, 0, 0, true)
		createProp(nodes[nodeID], 53, "maxSSRTemp", 0, 3600, 0, 0, true)

		createTemp(nodes[nodeID], 11, "temp", 2, true)
		createTemp(nodes[nodeID], 54, "ssrTemp", 2, true)

		return nodes[nodeID]

	} else if nodeID == 11 {
		nodes[nodeID] = &iot.IotNode{
			NodeId: nodeID,
			Name:   "serial1"}
		createProp(nodes[nodeID], 12, "temp", 2, 60, 0, 0, true)
		return nodes[nodeID]

	} else if nodeID == 14 {
		nodes[nodeID] = &iot.IotNode{
			NodeId: nodeID,
			Name:   "chint"}

		createProp(nodes[nodeID], 54, "total", 0, 60, 0, 0, true)
		createProp(nodes[nodeID], 55, "today", 2, 60, 0, 0, true)
		createProp(nodes[nodeID], 56, "temp", 1, 60, 0, 0, true)
		createProp(nodes[nodeID], 57, "power", 0, 60, 0, 0, true)

		return nodes[nodeID]

	} else if nodeID == 15 {
		nodes[nodeID] = &iot.IotNode{
			NodeId: nodeID,
			Name:   "sono"}

		createProp(nodes[nodeID], 9, "IP", 0, 0, 0, 0, true)
		createProp(nodes[nodeID], 10, "GPIO0", 0, 60, 0, 0, true)
		createProp(nodes[nodeID], 12, "relais", 0, 60, 0, 0, true)
		createProp(nodes[nodeID], 13, "button", 0, 60, 0, 0, true)
		createProp(nodes[nodeID], 15, "s15", 0, 0, 0, 0, true)

		return nodes[nodeID]

	} else if nodeID == 23 {
		nodes[nodeID] = &iot.IotNode{
			NodeId: nodeID,
			Name:   "afz"}

		createTemp(nodes[nodeID], 21, "buitenTemp", 2, true) // used by heating control!!
		createTemp(nodes[nodeID], 22, "douchTemp", 2, true)
		createTemp(nodes[nodeID], 23, "kraanTemp", 2, true)

		createTemp(nodes[nodeID], 24, "tempBinn", 2, true)          //"C,"
		createProp(nodes[nodeID], 25, "humBinn", 0, 60, 2, 0, true) //"%,"
		createProp(nodes[nodeID], 43, "absBinn", 0, 60, 2, 0, true) //"g/m3<br/>"

		createTemp(nodes[nodeID], 40, "tempBui", 2, true)          //"C,"
		createProp(nodes[nodeID], 41, "humBui", 0, 60, 2, 0, true) //"%,"
		createProp(nodes[nodeID], 42, "absBui", 0, 60, 2, 0, true) //"g/m3<br/>"

		createProp(nodes[nodeID], 44, "median", 2, 60, 1, 0, true) //"%"

		createProp(nodes[nodeID], 30, "manual", 0, 60, 0, 0, true)
		createProp(nodes[nodeID], 31, "fan", 0, 60, 1, 0, true)
		createProp(nodes[nodeID], 32, "maxHum", 0, 3600, 0, 0, true)
		createProp(nodes[nodeID], 33, "hotHum", 0, 3600, 0, 0, true)
		createProp(nodes[nodeID], 34, "coldHum", 0, 3600, 0, 0, true)
		createProp(nodes[nodeID], 35, "deltaHum", 0, 3600, 0, 0, true)

		return nodes[nodeID]

	} else {

		nodes[nodeID] = &iot.IotNode{
			NodeId: nodeID,
			Name:   "node" + strconv.Itoa(nodeID)}
		return nodes[nodeID]
	}
}

/*
	boot 	= getSensor(0, SensorType.Trace);
	uptime 	= getSensor(1);
	sample 	= getSensor(2, "sample", SensorType.Parm);

	freeMemProp = getSensor(4, "free");
	ping 	= getSensor(5, "ping");
	led 	= getSensor(50, "led", SensorType.Bool);

	netw70       = getSensor(70, "netw70"); //new IotSensor( this, device, 70, "netw70",0,1 );   // logLevel child side  not in code
	parentNetw   = getSensor(80, "netw80"); //new IotSensor( this, device, 80, "netw80"  ,0,1 );   // logLevel

	readCount    = getSensor(81, "rdCnt"); //new IotSensor( this, device, 81, "rdCnt"    ,0,1);

	sendCount    = getSensor(84, "sdCnt"); //new IotSensor( this, device, 84, "sdCnt"    ,0,1);
	sendErrCount = getSensor(85, "sdErrCnt"); //new IotSensor( this, device, 85, "sdErrCnt" ,0,1);
	sendErr      = getSensor(86, "sdErr"); //new IotSensor( this, device, 86, "sdErr"    ,0,1);

*/

func mqListenCmd(topic string) {

	fmt.Printf("listen: %s  \n", topic)

	// client := mqConnect("iotCmd", uri)
	client := iot.NewMQClient(iotConfig.MqttUri, "iotCmd")

	client.Subscribe(topic, 0, func(client mqtt.Client, msg mqtt.Message) {

		var payload = string(msg.Payload())
		// fmt.Printf("[%s]>%s\n", topic, payload)
		iotPayload := iot.ToPayload(payload)

		//command(&iotPayload)

		fmt.Printf("* cmd %s VarId %d ConnId %d Val %d Timestamp %d\n", iotPayload.Cmd, iotPayload.VarId, iotPayload.ConnId, iotPayload.Val, iotPayload.Timestamp)

		// TODO update timestamp conn node so we know it is still alive

	})
}

func mqListenUp(topic string) {

	fmt.Printf("listen: %s  \n", topic)

	// client := mqConnect("iotUp", uri)
	client := iot.NewMQClient(iotConfig.MqttUri, "iotUp")

	client.Subscribe(topic, 0, func(client mqtt.Client, msg mqtt.Message) {

		var payload = string(msg.Payload())
		//fmt.Printf("[%s]>%s", topic, payload)

		iotPayload := iot.ToPayload(payload)

		//fmt.Printf("* cmd %s NodeId %d PropId %d VarId %d ConnId %d Val %d Timestamp %d\n", rsp.Cmd, rsp.NodeId, rsp.PropId, rsp.VarId, rsp.ConnId, rsp.Val, rsp.Timestamp)

		if iotPayload.Cmd == "u" || iotPayload.Cmd == "U" {

			prop := prop(iotPayload.VarId)

			// if(!domo.nodesInitialized){
			// 	domo.logger.error("uploadVal: !domo.nodesInitialized for updateVal "+iotMsg._payload);
			// }

			//sensor.onUpdate
			//if( ! node.uploadVal(this, iotMsg) )return;

			if prop.ValStamp > iotPayload.Timestamp {
				fmt.Printf(`up err: timestamp < prop.timestamp for %d\n`, prop.VarId)
			}

			if prop.Val == iotPayload.Val &&
				prop.ValStamp == iotPayload.Timestamp {
				return // skip if duplicate messages because they cause trouble add-options for optimization
			}

			// additional check like min and max val or max delta

			// node.active = true;
			prop.Node.Dirty = true
			prop.Node.Timestamp = time.Now().Unix()

			if prop.PropId == 0 || prop.PropId == 1 {
				prop.Node.BootStamp = time.Now().Unix()
			}

			// node.persistMem();

			// //if(id==1485) logger.info("uploadVal.1485: retryCount = 0 ");
			updateLogItemSucces := false

			if prop.LogOption > 0 &&
				prop.PrevStamp > 0 {

				insertLogItem := false

				if prop.Val == iotPayload.Val &&
					prop.Val == prop.Prev {
					// fmt.Printf(" update logItem %d, %d, %d\n",prop.VarId, iotPayload.Val, iotPayload.Timestamp  )
					updateLogItemSucces = updateLogItem(prop, iotPayload.Val, iotPayload.Timestamp)

				} else if prop.LogOption > 1 {
					// if extrapolate 1 and 2 to 3. When 3 comes close then skip 2=the middle one
					partialTimeSpan := float64(prop.ValStamp - prop.PrevStamp)
					totalTimeSpan := float64(iotPayload.Timestamp - prop.PrevStamp)
					partialDelta := float64(prop.Val - prop.Prev)
					realDelta := float64(iotPayload.Val - prop.Prev)
					calculatedDelta := partialDelta * totalTimeSpan / partialTimeSpan
					if math.Abs(realDelta-calculatedDelta) < 0.6 {
						updateLogItemSucces = updateLogItem(prop, iotPayload.Val, iotPayload.Timestamp)
					} else {
						insertLogItem = true
					}

				} else {
					insertLogItem = true
				}

				if insertLogItem || !updateLogItemSucces {
					// fmt.Printf(" append logItem %d, %d, %d\n",prop.VarId, iotPayload.Val, iotPayload.Timestamp  )
					appendLogItem(prop, iotPayload.Val, iotPayload.Timestamp)
				}
			}

			if !updateLogItemSucces {
				prop.Prev = prop.Val
				prop.PrevStamp = prop.ValStamp
			}

			prop.Val = iotPayload.Val
			prop.ValStamp = iotPayload.Timestamp

			prop.RetryCount = 0

			// if(status < 0 && iotMsg.val == soll )
			// {
			// 	status = - status;
			// }
			// implement tracing

			// onChange(iotMsg.val,  iotMsg.timestamp);

			prop.MemDirty = true

			prop.IsNew = false
			if iotPayload.Timestamp > prop.Node.Timestamp {
				prop.Node.Timestamp = iotPayload.Timestamp
				prop.Node.ConnId = iotPayload.ConnId
				prop.Node.Dirty = true
			}
			prop.InErr = false

			persistPropMem(prop)

			mvcUp(prop)

		} else if iotPayload.Cmd == "e" || iotPayload.Cmd == "E" {

			prop := prop(iotPayload.VarId)
			prop.MemDirty = true
			prop.IsNew = false
			prop.Err = iotPayload.Val
			prop.InErr = true
			prop.ErrStamp = time.Now().Unix()
			prop.Node.Timestamp = time.Now().Unix()
			prop.Node.ConnId = iotPayload.ConnId

			persistPropMem(prop)

			// } else if iotPayload.Cmd == "S" || iotPayload.Cmd == "s" {
			// should be gone when cmd queue is operational
		} else {
			fmt.Printf("skip %s [%s]\n", payload, msg.Topic())

		}
	})
}

func mvcUp(prop *iot.IotProp) {

	if prop.GlobalMvc {
		topic := fmt.Sprintf(`node/%d/global`, prop.NodeId)
		payload := fmt.Sprintf(`{"mvcupload":{"p%d_%d":{"v":%d,"r":1}}}`, prop.NodeId, prop.PropId, prop.Val)
		// fmt.Printf("pub %s [%s]\n", payload, topic)

		token := mqttClient3b.Publish(topic, byte(0), false, payload)
		if token.Wait() && token.Error() != nil {
			fmt.Printf("error %s [%s]\n", payload, topic)
			fmt.Printf("Fail to publish, %v", token.Error())
		}
	}

	if prop.GlobalMvc ||
		prop.LocalMvc {

		topic := fmt.Sprintf(`node/%d/local`, prop.NodeId)
		payload := fmt.Sprintf(`{"mvcupload":{"p%d":{"v":%d,"r":1}}}`, prop.PropId, prop.Val)
		// fmt.Printf("pub %s [%s]\n", payload, topic)

		token := mqttClient3b.Publish(topic, byte(0), false, payload)
		if token.Wait() && token.Error() != nil {
			fmt.Printf("error %s [%s]\n", payload, topic)
			fmt.Printf("Fail to publish, %v", token.Error())
		}
	}

}

func appendLogItem(prop *iot.IotProp, newVal int64, timeStamp int64) bool {

	append, err := database2.Prepare(`
	INSERT INTO logMem( varid, stamp, val, delta ) VALUES(?, ?, ?, ?) 
	ON DUPLICATE KEY UPDATE val=?, delta=? `)

	defer append.Close()

	if err != nil {
		fmt.Printf("appendLogItem.err: %v\n", err.Error())
		return false
	}

	delta := newVal - prop.Val
	_, err = append.Exec(prop.VarId, timeStamp, newVal, delta, newVal, delta)

	if err != nil {
		fmt.Printf("appendLogItem.err: %v\n", err.Error())
		return false
	}

	return true
}

func updateLogItem(prop *iot.IotProp, newVal int64, newTimeStamp int64) bool {

	update, err := database2.Prepare(`Update logMem set stamp = ?, val = ?, delta = ? where varid = ? and stamp = ?`)

	defer update.Close()

	if err != nil {
		fmt.Printf("updateLogItem.err: %v\n", err.Error())
		return false
	}

	delta := newVal - prop.Val
	_, err = update.Exec(newTimeStamp, newVal, delta, prop.VarId, prop.ValStamp)

	if err != nil {
		fmt.Printf("updateLogItem.err: %v\n", err.Error())
		return false
	}
	return true
}

// func onPropNewVal(prop *iot.IotProp, iotPayload){

// }

func persistNodeDef(node *iot.IotNode) {

	persist, err := database2.Prepare(`
	INSERT INTO nodesDef( nodeId, 
		name, 
		connId ) VALUES(?, ?, ? ) 
	ON DUPLICATE KEY UPDATE name=?, 
		connId=? `)
	if err != nil {
		fmt.Printf("persistNodeDef: %v", err.Error())
	}

	defer persist.Close()

	_, err = persist.Exec(
		node.NodeId,
		node.Name,
		node.ConnId,
		node.Name,
		node.ConnId)

	if err != nil {
		fmt.Printf("persistNodeDef: %v", err.Error())
	}
	node.DefDirty = false
}

func persistNodeMem(node *iot.IotNode) {

	persist, err := database2.Prepare(`
	INSERT INTO nodesMem( nodeId, 
		bootCount, 
		freeMem,
		bootTime,
		timestamp ) VALUES(?, ?, ?, ?, ? ) 
	ON DUPLICATE KEY UPDATE bootCount=?, 
		freeMem=?,
		bootTime=?,
		timestamp=? `)
	if err != nil {
		fmt.Printf("persistNodeMem: %v", err.Error())
	}

	defer persist.Close()

	_, err = persist.Exec(
		node.NodeId,
		node.BootCount,
		node.FreeMem,
		node.BootStamp,
		node.Timestamp,
		node.BootCount,
		node.FreeMem,
		node.BootStamp,
		node.Timestamp)

	if err != nil {
		fmt.Printf("persistNodeMem: %v", err.Error())
	}
	node.Dirty = false
}

func persistPropDef(prop *iot.IotProp) {

	persist, err := database2.Prepare(`
	INSERT INTO propsDef( varid, 
		nodeId, 
		propId, 
		name, 
		decimals, 
		logOption, 
		traceOption, 
		localMvc,
		globalMvc,
		drawType,
		drawOffset,
		drawColor,
		drawFactor,
		refreshRate ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
	ON DUPLICATE KEY UPDATE nodeId=?, 
		propId=?, 
		name=?, 
		decimals=?, 
		logOption=?, 
		traceOption=?, 
		localMvc=?,
		globalMvc=?,
		drawType=?,
		drawOffset=?,
		drawColor=?,
		drawFactor=?,
		refreshRate=? `)
	if err != nil {
		fmt.Printf("persistPropDef: %v", err.Error())
	}

	defer persist.Close()

	_, err = persist.Exec(
		prop.VarId,
		prop.NodeId,
		prop.PropId,
		prop.Name,
		prop.Decimals,
		prop.LogOption,
		prop.TraceOption,
		prop.LocalMvc,
		prop.GlobalMvc,
		prop.DrawType,
		prop.DrawOffset,
		prop.DrawColor,
		prop.DrawFactor,
		prop.RefreshRate,
		prop.NodeId,
		prop.PropId,
		prop.Name,
		prop.Decimals,
		prop.LogOption,
		prop.TraceOption,
		prop.LocalMvc,
		prop.GlobalMvc,
		prop.DrawType,
		prop.DrawOffset,
		prop.DrawColor,
		prop.DrawFactor,
		prop.RefreshRate)

	if err != nil {
		fmt.Printf("persistPropDef: %v", err.Error())
	}
	prop.DefDirty = false
}

func persistPropMem(prop *iot.IotProp) {

	persist, err := database2.Prepare(`
	INSERT INTO propsMem( varid, 
		val, 
		valString, 
		valStamp, 
		inErr, 
		err, 
		errStamp, 
		retryCnt, 
		nextRefresh) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?) 
	ON DUPLICATE KEY UPDATE val=?, 
		valString=?, 
		valStamp=?, 
		inErr=?, 
		err=?, 
		errStamp=?, 
		retryCnt=?, 
		nextRefresh=? `)

	if err != nil {
		fmt.Printf("persistPropMem: %v", err.Error())
	}

	defer persist.Close()

	var nextRefresh int64
	if prop.RefreshRate > 0 {
		nextRefresh = time.Now().Unix() + int64(prop.RefreshRate)
	}

	_, err = persist.Exec(
		prop.VarId,
		prop.Val,
		prop.ValString,
		prop.ValStamp,
		prop.InErr,
		prop.Err,
		prop.ErrStamp,
		prop.RetryCount,
		nextRefresh,
		prop.Val,
		prop.ValString,
		prop.ValStamp,
		prop.InErr,
		prop.Err,
		prop.ErrStamp,
		prop.RetryCount,
		nextRefresh)

	if err != nil {
		fmt.Printf("persistPropMem: %v", err.Error())
	}

	prop.MemDirty = false
}

func nodeId(varId int) int {

	if varId < iot.IDFactor {
		// fmt.Printf("Warn util.nodeId: %s util.nodeId:  Id "+varId+" <iot.IDFactor!"\n", payload)
		//logger.error("util.nodeId:  Id "+varId+" <iot.IDFactor!");
		return varId
	}
	return varId / iot.IDFactor
}

func propId(varId int) int {

	return varId % iot.IDFactor
}

//----------------------------------------------------------------------------------------

func startIotService(service string) error {
	var err error
	var tcpAddr *net.TCPAddr

	tcpAddr, err = net.ResolveTCPAddr("tcp4", service)
	if err != nil {
		return err
	}
	//    checkError(err)

	tcpListener, errr := net.ListenTCP("tcp4", tcpAddr)
	if errr != nil {
		return errr
	}
	//    checkError(err)

	// fmt.Println("DbService: " + service  )
	fmt.Printf("iot: %s \n", service)

	go iotServiceListener(tcpListener)

	return err
}

func iotServiceListener(listener *net.TCPListener) {

	for {
		conn, err := listener.Accept()
		fmt.Println("iotSvc accept:")
		if err != nil {
			continue
		}

		// multi threading:
		go handleIotServiceRequest(conn)
	}
}

func handleIotServiceRequest(conn net.Conn) {

	//TODO prevent long request attack
	fmt.Println("new iotSvc conn:")

	buff := make([]byte, 1024)
	// TODO
	// var reqBuff bytes.Buffer

	var payload string
	var iotPayload iot.IotPayload
	var svcLoop = "startServiceLoop"
	var nulByte = make([]byte, 0)

	clientAddr := conn.RemoteAddr().String()
	mqClientID := fmt.Sprintf("iotClnt%s", clientAddr[strings.Index(clientAddr, ":")+1:len(clientAddr)])
	mqClient := iot.NewMQClient(iotConfig.MqttUri3b, mqClientID)
	subscription := ""

	// fmt.Printf("iotClientID:%s\n", mqClientID)

	defer conn.Close() // close connection before exit

	for svcLoop != "stop" {

		conn.SetReadDeadline(time.Now().Add(49 * time.Second)) // 49

		n, err := conn.Read(buff)

		if err != nil {

			if err.Error() == "EOF" {
				return
			}

			fmt.Printf("iotSvc readErr:%v\n", err.Error())
			return
		}

		payload = strings.TrimSpace(string(buff[0:n]))
		iotPayload = iot.ToPayload(payload)

		// fmt.Printf("iotSvc.payload<%s\niotPayload:%+v\n\n", payload, iotPayload)
		fmt.Printf("iotSvc.payload<%s\n", payload)

		switch iotPayload.Cmd {

		case "close", "stop":
			svcLoop = "stop"
			conn.Write(nulByte)

		case "subscribe":

			// implement subscribe to
			// how to re-use conn ???
			topic := iotPayload.Parms[1]

			fmt.Printf("%s>%s\n", iotPayload.Cmd, topic)

			mqSubscribe(mqClient, topic, conn, subscription)
			// mqClient.Subscribe(topic, 0, func(client mqtt.Client, msg mqtt.Message) {

			// 	mqPayload := string(msg.Payload())
			// 	fmt.Printf("%s[%s]\n", mqPayload, topic)

			// 	// fmt.Printf("* cmd %s VarId %d ConnId %d Val %d Timestamp %d\n", iotPayload.Cmd, iotPayload.VarId, iotPayload.ConnId, iotPayload.Val, iotPayload.Timestamp)

			// 	conn.Write([]byte(mqPayload + "\n"))
			// })
			conn.Write(nulByte)

		default:
			commandAndRespond(&iotPayload, conn)

			if iotPayload.Cmd == "mvcdata" {

				topic := `node/+/global`

				if iotPayload.Parms[1] == "nodeLocal" ||
					iotPayload.Parms[1] == "localNode" {

					topic = fmt.Sprintf(`node/%d/#`, iotPayload.NodeId)

				}

				//fmt.Printf("%s:topic[%s]\n", iotPayload.Cmd, topic)

				mqSubscribe(mqClient, topic, conn, subscription)
				subscription = topic

			}
		}
	}
}

func mqSubscribe(client mqtt.Client, topic string, conn net.Conn, prevTopic string) {

	if topic == prevTopic {
		return
	}

	if len(prevTopic) > 0 {

		if token := client.Unsubscribe(prevTopic); token.Wait() && token.Error() != nil {
			fmt.Println(token.Error())
			os.Exit(1)
		}
		// fmt.Printf("Unsubscribe [%s] \n", prevTopic)
	}

	// fmt.Printf("client.IsConnected:%t \n", client.IsConnected())

	// if token := client.Subscribe(topic, 0, func(clnt mqtt.Client, msg mqtt.Message) {

	// 	mqPayload := string(msg.Payload())

	// 	fmt.Printf("%s[%s]\n", mqPayload, topic)

	// 	conn.Write([]byte(mqPayload + "\n"))

	// }); token.Wait() && token.Error() != nil {
	// 	fmt.Println(token.Error())
	// 	os.Exit(1)
	// }

	token := client.Subscribe(topic, 0, func(clnt mqtt.Client, msg mqtt.Message) {

		mqPayload := string(msg.Payload())

		// fmt.Printf("%s[%s]\n", mqPayload, topic)

		conn.Write([]byte(mqPayload + "\n"))
	})

	if token.Error() != nil {
		fmt.Println(token.Error())
		// fmt.Printf("Subscribe.err %s:topic[%s]\n", token.Error(), topic)s
	}

	// fmt.Printf("Subscribe [%s] \n", topic)
}

func commandAndRespond(iotPayload *iot.IotPayload, conn net.Conn) {

	// https://gobyexample.com/channels
	// https://medium.com/learning-the-go-programming-language/streaming-io-in-go-d93507931185

	var respBuff bytes.Buffer

	switch iotPayload.Cmd {

	case "mvcdata":

		mvcData(&respBuff, iotPayload)

	default:
		respBuff.Write(
			[]byte(
				command(iotPayload)))
	}

	//fmt.Printf("commandAndRespond: %s", respBuff.String())

	conn.Write(respBuff.Bytes())
}

func mvcData(respBuff *bytes.Buffer, iotPayload *iot.IotPayload) {

	fmt.Printf("mvcData %s\n", iotPayload.Parms[1])

	if iotPayload.Parms[1] == "global" {
		mvcDataGlobal(respBuff)

	} else if iotPayload.Parms[1] == "node" || iotPayload.Parms[1] == "nodeGlobal" {
		mvcDataNode(respBuff, true, iotPayload)

	} else if iotPayload.Parms[1] == "nodeLocal" || iotPayload.Parms[1] == "localNode" {
		mvcDataNode(respBuff, false, iotPayload)

	} else {
		fmt.Printf("mvcData: skip %v ", iotPayload.Parms)
		respBuff.Write(
			[]byte(
				fmt.Sprintf(`{"retcode":0,"message":"command: skip %v"}`, iotPayload.Parms)))

	}
}
func mvcDataGlobal(respBuff *bytes.Buffer) {
	// var err error

	respBuff.Write([]byte(fmt.Sprintf(`{"mvcdata3":{"log_From":["%d"]`, 0)))

	// mvc3(conn, "s2__10", "0", 0)
	propMvc3(respBuff, getProp(2, 10), true)

	for nodeId, node := range nodes {

		background := "Red" //

		if node.NodeId == 2 {
			background = "green"

		} else if node.Timestamp == 0 {
			background = "Cyan"

		} else if node.Timestamp >= time.Now().Unix()-60 {
			background = "green"

		} else if node.Timestamp >= time.Now().Unix()-120 {
			background = "yellow"
		}
		respBuff.Write([]byte(fmt.Sprintf(`,"n%d":["%d",%d,"%s"]`, nodeId, nodeId, 0, background)))

		// fmt.Println("k:", nodeId, "v:", node)
	}

	for _, prop := range props {
		if prop.GlobalMvc {
			//fmt.Println("mvcDataGlobal varID:", varID, "prop:", prop)

			propMvc3(respBuff, prop, true)

		}
	}

	respBuff.Write([]byte("}}"))

	// conn.Write([]byte(fmt.Sprintf(`],"count":%d,"start":%d,"stop":%d,"stopval":%d}`, cnt, startTimestamp, timestamp, val)))

	// fmt.Printf(`timedata: ],"count":%d,"start":%d,"stop":%d,"stopval":%d}\n\n`, cnt, startTimestamp, timestamp, val)

	respBuff.Write([]byte("\n"))
}

func propMvc3(respBuff *bytes.Buffer, prop *iot.IotProp, global bool) {

	if !prop.IsNew {

		recent := 01 // 2 oranje, 3 rood

		if prop.RefreshRate > 0 {
			recent = 3

			if prop.ValStamp >= time.Now().Unix()-int64(2*prop.RefreshRate) {
				recent = 1

			} else if prop.ValStamp >= time.Now().Unix()-int64(3*prop.RefreshRate) {
				recent = 2
			}
		}

		if prop.Decimals == 0 {
			if global {
				respBuff.Write([]byte(fmt.Sprintf(`,"p%d_%d":[%d,%d]`, prop.NodeId, prop.PropId, prop.Val, recent)))
			} else {
				respBuff.Write([]byte(fmt.Sprintf(`,"p%d":[%d,%d]`, prop.PropId, prop.Val, recent)))
			}

		} else if prop.Decimals == 1 || prop.Decimals == 2 || prop.Decimals == 3 {

			factor := math.Pow10(prop.Decimals)

			if global {
				respBuff.Write([]byte(fmt.Sprintf(`,"p%d_%d":[%.1f,%d]`, prop.NodeId, prop.PropId, float64(prop.Val)/factor, recent)))
			} else {
				respBuff.Write([]byte(fmt.Sprintf(`,"p%d":[%.1f,%d]`, prop.PropId, float64(prop.Val)/factor, recent)))
			}

		} else {
			if global {
				respBuff.Write([]byte(fmt.Sprintf(`,"p%d_%d":[%d,%d]`, prop.NodeId, prop.PropId, prop.Val, recent)))
			} else {
				respBuff.Write([]byte(fmt.Sprintf(`,"p%d":[%d,%d]`, prop.PropId, prop.Val, recent)))
			}
		}
	}
}

func command(iotPayload *iot.IotPayload) string {

	fmt.Printf("command: %v\n", iotPayload)

	switch iotPayload.Cmd {

	case "ping":
		fmt.Printf("pong\n")
		if iotPayload.IsJson {
			// writer.Write([]byte(fmt.Sprintf(`{"retcode":0,"message":"pong"}`))
			return fmt.Sprintf(`{"retcode":0,"message":"pong"}`)
		} else {
			// writer.Write([]byte(fmt.Sprintf(`pong`))
			return fmt.Sprintf(`pong`)
		}
		// writer.Write([]byte(
	case "active", "sample", "s02", "s50", "led", "sensors", "mvcNode", "mvcSensor":
		//logger.info("------------- active 2="+parm2+" 3="+parm3+"  --------------" );
		// active = parm1.equalsIgnoreCase("true");
		fmt.Printf("command: skip %v ", iotPayload.Parms)
		return fmt.Sprintf(`{"retcode":0,"message":"command: skip %v"}`, iotPayload.Parms)

	case "logDel": // trace delete
		// //		 		logger.info("------ action logDel "+device );
		// 			domo.db.executeUpdate("delete from domoTrace where van ="+nodeId);
		// 			domo.db.executeUpdate("delete from domoTraceMem where van ="+nodeId );
	}

	if iotPayload.NodeId == 2 || iotPayload.NodeId == 0 {
		return localCommand(iotPayload)
	}

	switch iotPayload.Cmd {

	case "S", "R", "O", "N", "B", "T", "b":
		down(iotPayload)

	default:
		return fmt.Sprintf(`{"retcode":99,"message":"cmd %s not found"}`, iotPayload.Cmd)
	}

	return fmt.Sprintf(`{"retcode":0,"message":"cmd %s"}`, iotPayload.Cmd)
}

// [iotOut/11] {S,4,52,1,55}
func down(iotPayload *iot.IotPayload) {

	prop := prop(iotPayload.VarId)
	if prop.Node.ConnId < 1 {
		fmt.Printf("down.err: connId < 1 for %v\n", iotPayload)

	} else {
		topic := iotConfig.MqttDown + "/" + strconv.Itoa(prop.Node.ConnId)
		payload := fmt.Sprintf("{%s,%d,%d,1,%d}", iotPayload.Cmd, iotPayload.NodeId, iotPayload.PropId, iotPayload.Val)
		//fmt.Printf("down %s [%s]\n", payload, topic)
		token := mqttClient.Publish(topic, byte(0), false, payload)
		if token.Wait() && token.Error() != nil {
			fmt.Printf("error %s [%s]\n", payload, iotConfig.MqttDown)
			fmt.Printf("Fail to publish, %v", token.Error())
		}
	}
}

func localCommand(iotPayload *iot.IotPayload) string {

	// fmt.Printf("localCommand: %v\n", iotPayload)
	// forward to the old iotSvc

	switch iotPayload.Cmd {

	case "S":
		prop := prop(iotPayload.VarId) // test exist ???

		// Thuis
		// iotSvc.payload<setVal&2&20&1
		// if prop.PropId == 20 || prop.PropId == 21 {

		payload := fmt.Sprintf("setVal&%d&%d&%d", prop.NodeId, prop.PropId, iotPayload.Val)
		// fmt.Printf("setval: %s>%s\n", iotConfig.IotSvcOld, payload)

		iot.Send(&raspbian3, payload)

		// checkError(errr)

		// 	return fmt.Sprintf(`{"retcode":0,"message":"varId %d handled by iotOld"}`, iotPayload.VarId)
		// }

		if prop.Val == iotPayload.Val &&
			prop.ValStamp == iotPayload.Timestamp &&
			iotPayload.Timestamp > 0 {

			fmt.Printf("localCommand:skip equal update\n")
			return fmt.Sprintf(`{"retcode":0,"message":"varId %d duplicate Set"}`, iotPayload.VarId)
		}

		if prop.ValStamp > iotPayload.Timestamp &&
			iotPayload.Timestamp > 0 {

			fmt.Printf("localCommand:skip new timestamp to old!\n")
			return fmt.Sprintf(`{"retcode":0,"message":"varId %d skip Set aged Set"}`, iotPayload.VarId)
		}

		prop.MemDirty = true
		prop.IsNew = false
		if prop.Decimals >= 0 {
			prop.Val = iotPayload.Val
			if iotPayload.Timestamp > 0 {
				prop.ValStamp = iotPayload.Timestamp
			} else {
				prop.ValStamp = time.Now().Unix()
			}

		} else {
			//	prop.ValString
		}

	case "R":
		return fmt.Sprintf(`{"retcode":0,"message":"cmd %s no need for localCommand"}`, iotPayload.Cmd)

	default:
		return fmt.Sprintf(`{"retcode":99,"message":"cmd %s not found"}`, iotPayload.Cmd)
	}

	return fmt.Sprintf(`{"retcode":0,"message":"cmd %s"}`, iotPayload.Cmd)
}

// func mvcDataOBS(conn net.Conn, iotPayload *iot.IotPayload) {

// 	if iotPayload.Parms[1] == "global" {
// 		mvcDataGlobalOBS(conn)

// 	} else if iotPayload.Parms[1] == "node" || iotPayload.Parms[1] == "nodeGlobal" {
// 		mvcDataNode(conn, true, iotPayload)

// 	} else if iotPayload.Parms[1] == "nodeLocal" || iotPayload.Parms[1] == "localNode" {
// 		mvcDataNode(conn, false, iotPayload)

// 	} else {
// 		fmt.Printf("mvcData: skip %v ", iotPayload.Parms)
// 		conn.Write(
// 			[]byte(
// 				fmt.Sprintf(`{"retcode":0,"message":"command: skip %v"}`, iotPayload.Parms)))

// 	}
// }

func mvcDataNode(writer *bytes.Buffer, global bool, iotPayload *iot.IotPayload) {

	// ,"up":{"v":"166:43"},"last":{"v":"0:0:3"},"active":{"v":1},
	// 		socketOut.print (",\"active\":{\"v\":"+((active)?1:0)+"}");

	node := getNode(iotPayload.NodeId)

	fmt.Printf("mvcDataNode.nodeId:%d\n", iotPayload.NodeId)
	writer.Write([]byte(fmt.Sprintf(`{"mvcdata3":{"id":["%d"]`, iotPayload.NodeId)))

	writer.Write([]byte(fmt.Sprintf(`,"caption":["%s"]`, node.Name)))
	writer.Write([]byte(fmt.Sprintf(`,"html":["%s"]`, node.Name)))

	if node.Timestamp > 0 {
		// diff := now().Sub(node.Timestamp)
		duration := time.Now().Sub(time.Unix(node.Timestamp, 0))
		writer.Write([]byte(fmt.Sprintf(`,"last":["%s"]`, iot.FmtMMSS(duration))))
	}

	for _, prop := range props {

		if prop.NodeId == iotPayload.NodeId {
			propMvc3(writer, prop, global)
		}
		// fmt.Println("k:", nodeId, "v:", node)
	}

	if iotPayload.NodeId == 7 {
		mvcP1(writer, global, iotPayload)

	} else if iotPayload.NodeId == 8 {

	}

	writer.Write([]byte("}}"))
	writer.Write([]byte("\n"))

	// 		 socketOut.print (",\"retCode\":{\"v\":0"+"}");
	// //			 if(open) socketOut.print("}");
	// 	} catch (Exception ex) {
	// 		logger.error("mvcData2?node ex=" + ex.getMessage());
	// 		socketOut.print (",\"retCode\":{\"v\":-1"+"}");
	// 		socketOut.print (",\"error\":{\"v\":\""+ex.getMessage()+"\"}");

}

func mvcP1(writer *bytes.Buffer, global bool, iotPayload *iot.IotPayload) {

	// String strIp="?:?:?:?";
	// long ip = s9.ist;

	// if(s9.ist>0)
	// {
	// 	long ip4 = ip % 256L;
	// 	ip = ip / 256;
	// 	long ip3 = ip % 256L;
	// 	ip = ip / 256;
	// 	long ip2 = ip % 256L;
	// 	long ip1 = ip = ip / 256L;

	// 	strIp = ip4 + "." + ip3 + "." + ip2 + "." + ip1;
	// }

	// socketOut.print ("{\"mvcdata2\":{\"ip\":{\"v\":\""+strIp +"\"}");
	// socketOut.print (",\"iphref\":{\"v\":\"http://"+strIp +"/up\"}");
	// super.mvcLocalData(socketOut, false);

}

func mvcKetel(writer *bytes.Buffer, global bool, iotPayload *iot.IotPayload) {

	// socketOut.print (",\"xtrVWTijd\":{\"v\":"+domo.verw.onPeriode+"}");
	// socketOut.print (",\"xtrVWPauze\":{\"v\":"+domo.verw.offPeriode+"}");

}

// func mvcDataGlobalOBS(conn net.Conn) {
// 	// var err error

// 	conn.Write([]byte(fmt.Sprintf(`{"mvcdata3":{"log_From":["%d"]`, 0)))

// 	// mvc3(conn, "s2__10", "0", 0)
// 	propMvc3(conn, getProp(2, 10), true)

// 	for nodeId, node := range nodes {

// 		background := "Red" //

// 		if node.NodeId == 2 {
// 			background = "green"

// 		} else if node.Timestamp == 0 {
// 			background = "Cyan"

// 		} else if node.Timestamp >= time.Now().Unix()-60 {
// 			background = "green"

// 		} else if node.Timestamp >= time.Now().Unix()-120 {
// 			background = "yellow"
// 		}
// 		conn.Write([]byte(fmt.Sprintf(`,"n%d":["%d",%d,"%s"]`, nodeId, nodeId, 0, background)))

// 		// fmt.Println("k:", nodeId, "v:", node)
// 	}

// 	for _, prop := range props {
// 		if prop.GlobalMvc {
// 			//fmt.Println("mvcDataGlobal varID:", varID, "prop:", prop)

// 			propMvc3(conn, prop, true)

// 		}
// 	}

// 	// propMvc3(conn, getProp(4, 52), true) // dim level
// 	// propMvc3(conn, getProp(4, 52), true) // dim level
// 	// propMvc3(conn, getProp(4, 52), true) // dim level
// 	// propMvc3(conn, getProp(3, 13), true) // pomp
// 	// propMvc3(conn, getProp(3, 20), true) // aanvoer
// 	// propMvc3(conn, getProp(3, 11), true) // retour
// 	// propMvc3(conn, getProp(3, 12), true) // ruimte
// 	// propMvc3(conn, getProp(3, 14), true) // muurin
// 	// propMvc3(conn, getProp(3, 15), true) // afvoer
// 	// propMvc3(conn, getProp(3, 16), true) // keuken

// 	// propMvc3(conn, getProp(4, 52), true) // dim level
// 	// propMvc3(conn, getProp(4, 11), true) // Vin

// 	// propMvc3(conn, getProp(2, 20), true) // thuis
// 	// propMvc3(conn, getProp(2, 21), true) // vak
// 	// propMvc3(conn, getProp(2, 22), true) // testBtn
// 	// propMvc3(conn, getProp(2, 30), true) // TimerMode minutes or seconds

// 	// propMvc3(conn, getProp(11, 12), true) // Kamer temp

// 	// socketOut.print (",\"s2_33\":{\"v\":\""+iotSrv.timerMessage()+"\",\"r\":1}");

// 	// if(verw.active())
// 	// {
// 	// 	socketOut.print (",\"vwAan\":{\"v\":"+verw.onPeriode+",\"r\":1}");
// 	// 	socketOut.print (",\"vwUit\":{\"v\":"+verw.offPeriode+",\"r\":1}");
// 	// }
// 	// else
// 	// {
// 	// 	socketOut.print (",\"vwAan\":{\"v\":\"OFF\",\"r\":1}");
// 	// 	socketOut.print (",\"vwUit\":{\"v\":\"\",\"r\":1}");
// 	// }

// 	// propMvc3(conn, getProp(5, 24), true) // RetBad
// 	// propMvc3(conn, getProp(5, 25), true) // RetRechts
// 	// propMvc3(conn, getProp(5, 26), true) // RetLinks
// 	// propMvc3(conn, getProp(5, 41), true) // CVTemp
// 	// propMvc3(conn, getProp(5, 52), true) // thermostaat

// 	// propMvc3(conn, getProp(8, 11), true) // temp
// 	// propMvc3(conn, getProp(8, 54), true) // ssrTemp
// 	// propMvc3(conn, getProp(8, 55), true) // ssrPower
// 	// propMvc3(conn, getProp(8, 54), true) // total

// 	// propMvc3(conn, getProp(14, 54), true) // total
// 	// propMvc3(conn, getProp(14, 55), true) // today
// 	// propMvc3(conn, getProp(14, 56), true) // power
// 	// propMvc3(conn, getProp(14, 57), true) // CVTemp

// 	conn.Write([]byte("}}"))

// 	// conn.Write([]byte(fmt.Sprintf(`],"count":%d,"start":%d,"stop":%d,"stopval":%d}`, cnt, startTimestamp, timestamp, val)))

// 	// fmt.Printf(`timedata: ],"count":%d,"start":%d,"stop":%d,"stopval":%d}\n\n`, cnt, startTimestamp, timestamp, val)

// 	conn.Write([]byte("\n"))
// }

func propMvc(conn net.Conn, nodeId int, propId int) {

	prop := prop(nodeId*iot.IDFactor + propId)
	if !prop.IsNew {
		conn.Write([]byte(fmt.Sprintf(`,"s%s":{"v":"%d","r":1}`, prop.Name, prop.Val)))
	}
}

func mvc3(conn net.Conn, name string, val string, recent int) {

	if recent < 1 {
		conn.Write([]byte(fmt.Sprintf(`,"%s":["%s"]`, name, val)))
	} else {
		conn.Write([]byte(fmt.Sprintf(`,"%s":["%s",%d]`, name, val, recent)))
	}
}

func propMvc3OBS(conn net.Conn, prop *iot.IotProp, global bool) {

	if !prop.IsNew {

		recent := 01 // 2 oranje, 3 rood

		if prop.RefreshRate > 0 {
			recent = 3

			if prop.ValStamp >= time.Now().Unix()-int64(2*prop.RefreshRate) {
				recent = 1

			} else if prop.ValStamp >= time.Now().Unix()-int64(3*prop.RefreshRate) {
				recent = 2
			}
		}

		if prop.Decimals == 0 {
			if global {
				conn.Write([]byte(fmt.Sprintf(`,"p%d_%d":[%d,%d]`, prop.NodeId, prop.PropId, prop.Val, recent)))
			} else {
				conn.Write([]byte(fmt.Sprintf(`,"p%d":[%d,%d]`, prop.PropId, prop.Val, recent)))
			}

		} else if prop.Decimals == 1 || prop.Decimals == 2 || prop.Decimals == 3 {

			factor := math.Pow10(prop.Decimals)

			if global {
				conn.Write([]byte(fmt.Sprintf(`,"p%d_%d":[%.1f,%d]`, prop.NodeId, prop.PropId, float64(prop.Val)/factor, recent)))
			} else {
				conn.Write([]byte(fmt.Sprintf(`,"p%d":[%.1f,%d]`, prop.PropId, float64(prop.Val)/factor, recent)))
			}

		} else {
			if global {
				conn.Write([]byte(fmt.Sprintf(`,"p%d_%d":[%d,%d]`, prop.NodeId, prop.PropId, prop.Val, recent)))
			} else {
				conn.Write([]byte(fmt.Sprintf(`,"p%d":[%d,%d]`, prop.PropId, prop.Val, recent)))
			}
		}
	}
}

func connect(service string) (net.Conn, error) {

	conn, err := net.DialTimeout("tcp", service, 3*time.Second)

	if err == nil {
		// fmt.Printf("%s connected\r", service)

	} else {

		log.Fatal(fmt.Sprintf("iot: connect %s: %s", service, err.Error()))
	}

	return conn, err
}

func checkError(err error) {
	if err != nil {
		fmt.Printf("err: " + err.Error())
		fmt.Fprintf(os.Stderr, "iot Fatal error: %s", err.Error())
		os.Exit(1)
	}
}
