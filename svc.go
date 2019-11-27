/*   xxxx  xxxx

{"cmd":"mvcdata","type":"global"}
{"cmd":"timedata"}
{"cmd":"timedataDiff"}

0.0.0.0 tcp4 = IPv4 wildcard address

https://en.wikipedia.org/wiki/Time_series
dataPoints
timePoints

var reqBuff bytes.Buffer
reqBuff.WriteByte(char)
io.Copy(res, buf) // reads from buf, writes to res
*/

package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"strconv"

	"iot"
	// "iot/svc/verw"

	// "path"

	"runtime"
	"strings"
	"time"

	"github.com/tkanos/gonfig"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	_ "github.com/go-sql-driver/mysql"
)

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

type GraphPlan struct {
	Name string
	ID   int
}

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
	Debug     bool
	Logfile   string
	ServerId  string
}

var raspbian3 iot.IotConn
var iotConfig IotConfig
var mqttClient mqtt.Client
var mqttClient3b mqtt.Client

var props map[int]*iot.IotProp
var nodes map[int]*iot.IotNode

var graphPlansMap map[string]*GraphPlan

var (
	Detail  *log.Logger
	Trace   *log.Logger
	Info    *log.Logger
	Warning *log.Logger
	Error   *log.Logger
)

func init() {

}

func main() {

	fmt.Printf("... ")

	// verw.Setup()

	err := gonfig.GetConf(iot.Config(), &iotConfig)
	checkError(err)

	// logFile, err := os.OpenFile(iotConfig.Logfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	// if err != nil {
	// 	log.Fatalf("error opening file: %v", err)
	// }

	// defer logFile.Close()

	// fmt.Printf("%s ...\n", iotConfig.Logfile)

	InitLogging(ioutil.Discard, os.Stdout, os.Stdout, os.Stderr)

	if iotConfig.Debug {
		InitLogging(os.Stdout, os.Stdout, os.Stdout, os.Stderr)
		Trace.Print("Debug ...")
	}

	// log.SetOutput(logFile)

	// Info.Print("1...")
	// fmt.Printf("MqttCmd:%s \n", iotConfig.MqttCmd)

	nodes = make(map[int]*iot.IotNode)
	props = make(map[int]*iot.IotProp)
	graphPlansMap = make(map[string]*GraphPlan)
	// Info.Print("12..")
	database, err = sql.Open("mysql", iotConfig.Database)
	checkError(err)
	defer database.Close()
	// Info.Print("123.")
	database2, err = sql.Open("mysql", iotConfig.Database2)
	checkError(err)
	defer database2.Close()
	// Info.Print("1234")

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
	// Info.Print("2...")
	loadPlansFromDB()
	persistPlans()

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
	// Info.Print("3...")
	mqttClient = iot.NewMQClient(iotConfig.MqttUri, iotConfig.ServerId+"iotSvc")
	mqttClient3b = iot.NewMQClient(iotConfig.MqttUri3b, iotConfig.ServerId+"iotSvc")

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
	// Info.Print("4...")
	go mqListenUp(iotConfig.MqttUp)
	go mqListenCmd(iotConfig.MqttCmd)

	initVerw()
	go verwarming()

	// if runtime.GOOS == "windows" {
	// 	err = startIotService("localhost:" + iotConfig.Port)
	// } else {
	// 	err = startIotService("0.0.0.0:" + iotConfig.Port)
	// }
	// checkError(err)

	service := "0.0.0.0:" + iotConfig.Port
	if runtime.GOOS == "windows" {
		service = "localhost:" + iotConfig.Port
	}
	err = startIotService(service)
	checkError(err)

	Info.Printf("%s started", service)

	// reader := bufio.NewReader(os.Stdin)
	// scanner := bufio.NewScanner(os.Stdin)

	for true {

		// os.Stdin.SetReadDeadline(time.Now().Add(7 * time.Second)) // 49

		// text, _, errr := reader.ReadLine()

		time.Sleep(49 * time.Second) //49
		// fmt.Print(time.Now())
		// fmt.Println(": " + text)

		// if errr != nil {

		err = database.Ping()

		// fmt.Print(time.Now())
		// fmt.Println(" ping")

		// checkError(err)
		if err != nil {
			fmt.Printf("database.Ping() err: " + err.Error())
			fmt.Fprintf(os.Stderr, "iot: Fatal error: %s", err.Error())
			os.Exit(1)
		}

		err = database2.Ping()

		// fmt.Print(time.Now())
		// fmt.Println(" ping2")

		// checkError(err)
		if err != nil {
			fmt.Printf("database2.Ping() err: " + err.Error())
			fmt.Fprintf(os.Stderr, "iot: Fatal error: %s", err.Error())
			os.Exit(1)
		}

		// } else {

		// 	text = text

		// 	// resp, _ := iotCommand(myClient, "mvcdata&localNode&3\n")
		// 	// fmt.Printf("respons: %s", resp)

		// 	// resp = resp
		// 	// for scanner.Scan() {
		// 	// 	fmt.Print(time.Now())
		// 	// 	fmt.Println(scanner.Text())
		// 	// }

		// 	for k, v := range props {
		// 		fmt.Println("k:", k, "v:", v)
		// 	}

		// 	// for k, v := range nodes {
		// 	// 	fmt.Println("k:", k, "v:", v)
		// 	// }

		// 	fmt.Print("\niot: ")
		// }
	}

	fmt.Printf("webstop\n")
}

func getNode(nodeID int) *iot.IotNode {

	//fmt.Printf("getNode %d\n", nodeID)

	if node, ok := nodes[nodeID]; ok {
		return node

	} else {
		nodes[nodeID] = newNode(nodeID)
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
	client := iot.NewMQClient(iotConfig.MqttUri, iotConfig.ServerId+"iotCmd")

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
	client := iot.NewMQClient(iotConfig.MqttUri, iotConfig.ServerId+"iotUp")

	client.Subscribe(topic, 0, func(client mqtt.Client, msg mqtt.Message) {

		var payload = string(msg.Payload())

		Trace.Printf("Up [%s]%s\n", topic, payload)
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

	var buff bytes.Buffer
	// {"mvcup":{"p7_35":[0,1]}}
	// {"mvcupload":{"p7_22":{"v":684620,"r":1}}}

	if prop.GlobalMvc {
		topic := fmt.Sprintf(`node/%d/global`, prop.NodeId)
		propMvc3(&buff, prop, true)
		payload := fmt.Sprintf(`{"mvcup":{%s}}`, buff.String())

		// payload := fmt.Sprintf(`{"mvcupload":{"p%d_%d":{"v":%d,"r":1}}}`, prop.NodeId, prop.PropId, prop.Val)
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
		buff.Reset()
		propMvc3(&buff, prop, false)
		payload := fmt.Sprintf(`{"mvcup":{%s}}`, buff.String())

		// payload := fmt.Sprintf(`{"mvcupload":{"p%d":{"v":%d,"r":1}}}`, prop.PropId, prop.Val)
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

	Trace.Printf("mqMvcClient:%s\n", mqClientID)
	mqClient := iot.NewMQClient(iotConfig.MqttUri3b, iotConfig.ServerId+mqClientID)
	subscription := ""

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

			topic := iotPayload.Parms[1]

			fmt.Printf("%s>%s\n", iotPayload.Cmd, topic)
			mqSubscribe(mqClient, topic, conn, subscription)
			// ??? gaat het goed om alleen een nulByte terug te sturen???
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

		mqPayload := string(msg.Payload()) + "\n"

		// fmt.Printf("%s[%s]\n", mqPayload, topic)

		conn.Write([]byte(mqPayload))
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

	empty := respBuff.Len() < 1
	comma := ","
	if empty {
		comma = ""
	}

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
				respBuff.Write([]byte(fmt.Sprintf(`%s"p%d_%d":[%d,%d]`, comma, prop.NodeId, prop.PropId, prop.Val, recent)))
			} else {
				respBuff.Write([]byte(fmt.Sprintf(`%s"p%d":[%d,%d]`, comma, prop.PropId, prop.Val, recent)))
			}

		} else if prop.Decimals == 1 || prop.Decimals == 2 || prop.Decimals == 3 {

			factor := math.Pow10(prop.Decimals)

			if global {
				respBuff.Write([]byte(fmt.Sprintf(`%s"p%d_%d":[%.1f,%d]`, comma, prop.NodeId, prop.PropId, float64(prop.Val)/factor, recent)))
			} else {
				respBuff.Write([]byte(fmt.Sprintf(`%s"p%d":[%.1f,%d]`, comma, prop.PropId, float64(prop.Val)/factor, recent)))
			}

		} else {
			if global {
				respBuff.Write([]byte(fmt.Sprintf(`%s"p%d_%d":[%d,%d]`, comma, prop.NodeId, prop.PropId, prop.Val, recent)))
			} else {
				respBuff.Write([]byte(fmt.Sprintf(`%s"p%d":[%d,%d]`, comma, prop.PropId, prop.Val, recent)))
			}
		}
	}
}

func command(iotPayload *iot.IotPayload) string {

	fmt.Printf("command: %v\n", iotPayload)

	switch iotPayload.Cmd {

	case "ping":
		// fmt.Printf("pong\n")
		// if iotPayload.IsJson {
		// 	// writer.Write([]byte(fmt.Sprintf(`{"retcode":0,"message":"pong"}`))
		// 	return fmt.Sprintf(`{"retcode":0,"message":"pong"}`)
		// } else {
		// 	// writer.Write([]byte(fmt.Sprintf(`pong`))
		// 	return fmt.Sprintf(`pong`)
		// }
		// writer.Write([]byte(
		return ""
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
		Error.Println(err)
	}
}
func checkError2(err error) {
	if err != nil {
		fmt.Printf("err: " + err.Error())
		fmt.Fprintf(os.Stderr, "iot Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func InitLogging(
	traceHandle io.Writer,
	infoHandle io.Writer,
	warningHandle io.Writer,
	errorHandle io.Writer) {

	Detail = log.New(traceHandle,
		"",
		0)

	Trace = log.New(traceHandle,
		"",
		log.Ltime)

	Info = log.New(infoHandle,
		"",
		log.Ltime)

	Warning = log.New(warningHandle,
		"w)",
		log.Ldate|log.Ltime|log.Lshortfile)

	Error = log.New(errorHandle,
		"e)",
		log.Ldate|log.Ltime|log.Lshortfile)

}
