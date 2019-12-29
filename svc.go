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
	"io/ioutil"

	"log"
	"net"
	"os"

	"iot/verwarming"

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

// var DatabaseOld *sql.DB

type GraphPlan struct {
	Name string
	ID   int
}

type IotConfig struct {
	Port        string
	MqttUp      string
	MqttCmd     string
	MqttDown    string
	MqttUri     string
	DatabaseOld string
	DatabaseNew string
	IotSvcOld   string
	MqttUri3b   string
	Debug       bool
	Logfile     string
	ServerId    string
	Local       bool
}

var raspbian3 iot.IotConn
var iotConfig IotConfig
var mqttClient mqtt.Client
var mqttClient3b mqtt.Client

var graphPlansMap map[string]*GraphPlan

var refresherRunning bool
var nextRefresh int64
var prevRefresh int64

func init() {
	iot.LogDebug = false
}

func main() {

	fmt.Printf("...")

	err := gonfig.GetConf(iot.Config(), &iotConfig)
	checkError(err)

	fmt.Printf(" %s ...\n", iotConfig.Logfile)

	f, err := os.OpenFile(iotConfig.Logfile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644) //O_RDWR
	if err != nil {
		// fmt.Printf(" error reading config %s ...\n", iotConfig.Logfile)
		fmt.Printf("%s: %v", iotConfig.Logfile, err.Error())
	}

	defer f.Close()

	if iotConfig.Local {
		iot.InitLogging(os.Stdout, os.Stdout, os.Stdout, os.Stderr)

	} else {
		iot.InitLogging(ioutil.Discard, f, f, f)
	}

	/*
	  ioutil.Discard  os.Stdout  os.Stderr
	*/

	iot.Info.Print("****  start  ****")

	iot.MqttDown = iotConfig.MqttDown
	graphPlansMap = make(map[string]*GraphPlan)

	iot.DatabaseNew, err = sql.Open("mysql", iotConfig.DatabaseNew)
	checkError(err)
	defer iot.DatabaseNew.Close()
	iot.Info.Print("DB Connected")

	checkDB()

	iot.NodesFromDB()
	iot.PropsFromDB()
	iot.NodeValuesFromDB("nodesMem")
	iot.PropValuesFromDB("propsMem")

	iot.SvcNode = iot.GetOrDefaultNode(2, "svc")
	iot.LogLevel = iot.GetNodeProp(iot.SvcNode, 10)
	iot.TimerMode = iot.GetNodeProp(iot.SvcNode, 30)

	iot.ConnNode = iot.GetOrDefaultNode(1, "conn")

	iot.Pomp = iot.GetOrDefaultNode(3, "pomp")
	iot.PompTemp = iot.GetNodeProp(iot.Pomp, 20)

	iot.Dimmer = iot.GetOrDefaultNode(4, "dimmer")
	iot.Ketel = iot.GetOrDefaultNode(5, "ketel")
	iot.KetelSchakelaar = iot.GetNodeProp(iot.Ketel, 52)

	iot.P1 = iot.GetOrDefaultNode(7, "p1")
	iot.CloseIn = iot.GetOrDefaultNode(8, "closeIn")
	iot.Serial1 = iot.GetOrDefaultNode(11, "serial1")
	iot.KamerTemp = iot.GetNodeProp(iot.Serial1, 12)

	iot.Serial2 = iot.GetOrDefaultNode(12, "serial2")
	iot.Chint = iot.GetOrDefaultNode(14, "chint")
	// iot.Test = iot.GetOrDefaultNode(15, "sono")
	iot.Afzuiging = iot.GetOrDefaultNode(23, "afz")

	iot.Sonoff = iot.GetOrDefaultNode(15, "sonoff")
	iot.Switch = iot.GetNodeProp(iot.Sonoff, 12)
	iot.Sonoff2 = iot.GetOrDefaultNode(30, "sonoff2")

	iot.Neo31 = iot.GetOrDefaultNode(31, "neo31")
	iot.Kerstboom = iot.GetNodeProp(iot.Neo31, 12)

	iot.Son32 = iot.GetOrDefaultNode(32, "son32")
	iot.Son32Knop = iot.GetNodeProp(iot.Son32, 12)

	iot.Info.Print("Kernel initialized")

	raspbian3 = iot.IotConn{
		Service:        iotConfig.IotSvcOld,
		Protocol:       "tcp",
		ConnectTimeout: 3,
		ReadTimeout:    3,
		PingInterval:   21,
		Silent:         true,
	}

	initMq()

	// sync with connector needed???
	//iot.SendProp(iot.GetNodeProp(iot.ConnNode, 10), "S", iot.LogLevel.ValString)

	service := "0.0.0.0:" + iotConfig.Port
	if runtime.GOOS == "windows" {
		service = "localhost:" + iotConfig.Port
	}
	err = startIotService(service)
	checkError(err)

	verwarming.Setup(iot.GetNode(9))

	if !iotConfig.Local {
		go verwarming.Loop()
	}

	var nextWatchDog int64
	timerModeSecondes := false

	// iotConfig.Local = false

	motd := "NO SCHEDULER, NO REFRESHER, NO VERWARMING !!!"
	if !iotConfig.Local {
		motd = ""
	}

	fmt.Printf("Starting WatchDog ... %s\n", motd)

	for true {

		if nextWatchDog == 0 ||
			(nextWatchDog < time.Now().Unix() &&
				time.Now().Second()%10 == 0) {

			nextWatchDog = time.Now().Unix() + 7

			// iot.Trace.Printf("loglevel %s", iot.LogLevel.ValString)

			if !iotConfig.Local {
				if !schedulerRunning {
					go scheduler()
				}
				if !refresherRunning {
					go refresher()
				}
			} else {
				// fmt.Println("NO SCHEDULER, NO REFRESHER, NO VERWARMING !!!")

			}

			err = iot.DatabaseNew.Ping()
			if err != nil {
				iot.Err.Printf("DatabaseNew.Ping() err: %s", err.Error())
				// Error.Printf(os.Stderr, "iot: Fatal error: %s", err.Error())
				os.Exit(1)
			}
		}

		if time.Now().Second()%60 == 0 {
			mvcTimerUp()

		} else if iot.TimerMode.Val > 0 {
			timerModeSecondes = true
			mvcTimerUp()

		} else if timerModeSecondes {
			// one extra up to blank the seconds
			timerModeSecondes = false
			mvcTimerUp()
		}

		time.Sleep(995 * time.Millisecond)
	}

	fmt.Printf("webstop\n")
}

func mvcTimerUp() {

	strTime := fmt.Sprintf("%02d:%02d", time.Now().Hour(), time.Now().Minute())

	if iot.TimerMode.Val > 0 {
		strTime += fmt.Sprintf(":%02d", time.Now().Second())
	}

	//fmt.Printf("mvcTimerUp:%s\n", strTime)

	// payload := fmt.Sprintf(`{"mvcupload":{"n%dp%d":{"v":"%s","r":1}}}`, iot.TimerMode.NodeId, 33, strTime)
	payload := fmt.Sprintf(`{"mvcup":{"svcTimer":["%s",1]}}`, strTime)
	topic := fmt.Sprintf(`node/%d/global`, iot.TimerMode.NodeId)
	token := iot.MqttClient3b.Publish(topic, byte(0), false, payload)
	if token.Wait() && token.Error() != nil {
		fmt.Printf("error %s [%s]\n", payload, topic)
		fmt.Printf("Fail to publish, %v", token.Error())
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

//----------------------------------------------------------------------------------------
// use todo list in mem
// refresh one at the time

//----------------------------------------------------------------------------------------

func startIotService(service string) error {
	var err error
	var tcpAddr *net.TCPAddr

	tcpAddr, err = net.ResolveTCPAddr("tcp4", service)
	if err != nil {
		return err
	}

	tcpListener, errr := net.ListenTCP("tcp4", tcpAddr)
	if errr != nil {
		return errr
	}

	go iotServiceListener(tcpListener)

	iot.Info.Printf("%s started\n", service)

	return err
}

func iotServiceListener(listener *net.TCPListener) {

	for {
		conn, err := listener.Accept()

		if err != nil {
			continue
		}

		// multi threading:
		go handleIotServiceRequest(conn)
	}
}

func handleIotServiceRequest(conn net.Conn) {

	//TODO prevent long request attack
	//TODO  var reqBuff bytes.Buffer

	buff := make([]byte, 1024)

	var payload string
	var iotPayload iot.IotPayload
	var svcLoop = "startServiceLoop"
	var nulByte = make([]byte, 0)

	clientAddr := conn.RemoteAddr().String()
	mqClientID := fmt.Sprintf("%sisvc_%s", iotConfig.ServerId, clientAddr[strings.Index(clientAddr, ":")+1:len(clientAddr)])

	if iot.LogCode("s") {
		iot.Info.Printf("isvc.new clnt: %s\n", mqClientID)
	}

	mqClient := iot.NewMQClient(iotConfig.MqttUri3b, mqClientID)
	subscription := ""

	defer conn.Close() // close connection before exit

	for svcLoop != "stop" {

		conn.SetReadDeadline(time.Now().Add(49 * time.Second)) // 49

		n, err := conn.Read(buff)

		if err != nil {

			if err.Error() == "EOF" ||
				strings.HasSuffix(err.Error(), "closed by the remote host.") ||
				strings.HasSuffix(err.Error(), "connection reset by peer") {

				iot.Trace.Printf("isvc read:%s\n", err.Error())
				return
			}

			iot.Err.Printf("isvc read:%s\n", err.Error())
			return
		}

		payload = strings.TrimSpace(string(buff[0:n]))

		iotPayload = iot.ToPayload(payload)

		if iot.LogCode("s") || iot.LogPayload(&iotPayload) {
			iot.Info.Printf("isvc.payload:%s", payload)
			// iot.Detail.Printf("%+v", iotPayload)
		}

		switch iotPayload.Cmd {

		case "close", "stop":
			// iot.Info.Printf("svc.payload<%s\n", payload)
			svcLoop = "stop"
			conn.Write(nulByte)

		// case "ping":
		// 	conn.Write(nulByte)

		case "subscribe":

			topic := iotPayload.Parms[1]

			// iot.Trace.Printf("%s>%s\n", iotPayload.Cmd, topic)
			mqSubscribe(mqClient, topic, conn, subscription)
			// ??? gaat het goed om alleen een nulByte terug te sturen???
			conn.Write(nulByte)

		default:
			// iot.Trace.Printf("svc.payload<%s\n", payload)

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

func commandAndRespond(iotPayload *iot.IotPayload, conn net.Conn) {

	// https://gobyexample.com/channels
	// https://medium.com/learning-the-go-programming-language/streaming-io-in-go-d93507931185

	//fmt.Printf("commandAndRespond: \n")

	var respBuff bytes.Buffer

	switch iotPayload.Cmd {

	case "mvcdata":
		mvcData(&respBuff, iotPayload)

	case "timedata":
		timedata2(&respBuff, iotPayload)
		// conn.Write(respBuff.Bytes())
		// Detail.Printf("%s\n\n", respBuff.String())

	case "timedataDiff":
		timedataDiff2(&respBuff, iotPayload)
		// conn.Write(respBuff.Bytes())
		// Detail.Printf("%s\n\n", respBuff.String())

	case "timedataDeltas":
		timedataDeltas2(&respBuff, iotPayload)
		// conn.Write(respBuff.Bytes())
		// Detail.Printf("%s\n\n", respBuff.String())

	case "graphPlans":
		graphPlans(&respBuff)
		// conn.Write(respBuff.Bytes())
		// Detail.Printf("%s\n\n", respBuff.String())

	case "savePlans":
		savePlans(&respBuff, iotPayload)
		// conn.Write(respBuff.Bytes())

	case "graphs":
		graphs(&respBuff)
		// Detail.Printf("%s\n\n", respBuff.String())
		// conn.Write(respBuff.Bytes())

	case "graphs2":
		graphs2(&respBuff)
		// Detail.Printf("%s\n\n", respBuff.String())
		// conn.Write(respBuff.Bytes())

	default:
		respBuff.Write([]byte(command(iotPayload)))
	}

	//fmt.Printf("commandAndRespond: %s\n", iotPayload.Cmd)

	if respBuff.Len() < 1 {
		conn.Write([]byte("{\"retcode\":0}"))
	} else if respBuff.String()[0:1] == "{" {
		conn.Write(respBuff.Bytes())
	} else {
		resp := fmt.Sprintf("{\"retcode\":99,\"message\":\"%s\"}", respBuff.String())
		conn.Write([]byte(resp))
	}
}

func command(iotPayload *iot.IotPayload) string {

	if iot.LogCode("s") || iot.LogPayload(iotPayload) {
		iot.Info.Printf("isvc.command: %v", iotPayload)

	}

	switch iotPayload.Cmd {

	case "ping":
		return ""

	case "active", "sample", "s02", "s50", "led", "sensors", "mvcNode", "mvcSensor":
		//logger.info("------------- active 2="+parm2+" 3="+parm3+"  --------------" );
		// active = parm1.equalsIgnoreCase("true");
		iot.Err.Printf("command: skip %v ", iotPayload.Parms)
		return fmt.Sprintf(`{"retcode":0,"message":"command: skip %v"}`, iotPayload.Parms)

	case "ota", "O":
		return iot.SendNodeCmd("O", iotPayload.NodeId, "")

	case "R", "P":
		return iot.SendNodeCmd(iotPayload.Cmd, iotPayload.NodeId, "")

	case "set", "S":

		prop := iot.GetProp(iotPayload.NodeId, iotPayload.PropId)

		if iot.LogPayload(iotPayload) {
			iot.Info.Printf("isvc.command.set prop:%v\n", prop)
		}
		return iot.SetStrProp(prop, iotPayload.Val, iotPayload.Timestamp)
	}

	switch iotPayload.NodeId {

	case verwarming.NodeId:
		return verwarming.Command(iotPayload)

	case 0, 2:
		return localCommand(iotPayload)

	default:
		// case "R", "O", "N", "B", "T", "b":
		return iot.SendCmd(iotPayload)

	}

	// return fmt.Sprintf(`{"retcode":0,"message":"cmd %s"}`, iotPayload.Cmd)
}

func localCommand(iotPayload *iot.IotPayload) string {

	// fmt.Printf("localCommand: %v\n", iotPayload)
	// forward to the old iotSvc

	switch iotPayload.Cmd {

	case "R":
		return ""

	case "dim":

		// fmt.Printf("Dim %v\n", iotPayload)
		dimmerValue := iot.GetNodeProp(iot.Dimmer, 52)
		return iot.SendProp(dimmerValue, "S", iotPayload.Val)

	case "logDel": // trace delete
		// //		 		logger.info("------ action logDel "+device );
		// 			domo.db.executeUpdate("delete from domoTrace where van ="+nodeId);
		// 			domo.db.executeUpdate("delete from domoTraceMem where van ="+nodeId );
		fmt.Printf("command: skip %v ", iotPayload.Parms)
		return fmt.Sprintf(`{"retcode":0,"message":"command: skip %v"}`, iotPayload.Parms)

	case "logMem2Disk":

		optimizeLogMem()

		_, err := iot.DatabaseNew.Exec("call logMem2Disk()")

		if err != nil {
			fmt.Printf("logMem2Disk err:%v", err.Error())
			return fmt.Sprintf(`{"retcode":99,"message":"logMem2Disk error"}`)
		}

		return fmt.Sprintf(`{"retcode":0,"message":"logMem2Disk"}`)

	case "optimzeLogMem":

		optimizeLogMem()

		//return fmt.Sprintf(`{"retcode":0,"message":"optimzeLogMem"}`)

	case "cleanOldLog":

		deleteTill := time.Now().Unix() - 5529600

		sql := fmt.Sprintf("DELETE FROM logDsk WHERE stamp < %d", deleteTill)
		_, err := iot.DatabaseNew.Exec(sql)

		if err != nil {
			fmt.Printf("cleanOldLog err:%v \n", err.Error())
			return fmt.Sprintf(`{"retcode":99,"message":" %s"}`, iotPayload.Cmd)
		}

		// domo.db.executeUpdate ( "DELETE FROM domoTrace WHERE timestamp < "+deleteFrom );

	default:
		return fmt.Sprintf(`{"retcode":99,"message":"cmd %s not found"}`, iotPayload.Cmd)
	}

	return ""
}

func mvcKetel(writer *bytes.Buffer, global bool, iotPayload *iot.IotPayload) {

	// socketOut.print (",\"xtrVWTijd\":{\"v\":"+domo.verw.onPeriode+"}");
	// socketOut.print (",\"xtrVWPauze\":{\"v\":"+domo.verw.offPeriode+"}");

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
		iot.Err.Println(err)
	}
}
func checkError2(err error) {
	if err != nil {
		fmt.Printf("err: " + err.Error())
		fmt.Fprintf(os.Stderr, "iot Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func mqSubscribe(client mqtt.Client, topic string, conn net.Conn, prevTopic string) {

	if topic == prevTopic {
		return
	}

	if len(prevTopic) > 0 {

		if token := client.Unsubscribe(prevTopic); token.Wait() && token.Error() != nil {
			iot.Err.Println(token.Error())
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

		if iot.LogCode("s") {
			iot.Info.Printf("[%s]%s", topic, mqPayload)
		}

		conn.Write([]byte(mqPayload))
		conn.Write([]byte("\n"))

	})

	if token.Error() != nil {
		iot.Err.Println(token.Error())
		// fmt.Printf("Subscribe.err %s:topic[%s]\n", token.Error(), topic)s
	}

	// fmt.Printf("Subscribe [%s] \n", topic)
}
