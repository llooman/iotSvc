package main

import (
	"bytes"
	"fmt"
	"iot"
	"time"
)

func mvcData(respBuff *bytes.Buffer, iotPayload *iot.IotPayload) {

	// fmt.Printf("mvcData-%s\n", iotPayload.Parms[1])

	if iotPayload.Parms[1] == "global" {

		mvcDataGlobal(respBuff)

		if iot.LogCode("mvc") {
			iot.Info.Printf("mvcData-%s", iotPayload.Parms[1])
			iot.Detail.Print(respBuff.String())
		}
		return
	}

	node := iot.GetNode(iotPayload.NodeId)

	if iotPayload.Parms[1] == "node" || iotPayload.Parms[1] == "nodeGlobal" {
		// mvcDataNode(respBuff, true, iotPayload)
		iot.NodeMvc(respBuff, node, true)

	} else if iotPayload.Parms[1] == "nodeLocal" || iotPayload.Parms[1] == "localNode" {
		// mvcDataNode(respBuff, false, iotPayload)
		iot.NodeMvc(respBuff, node, false)

	} else {
		iot.Err.Printf("mvcData: skip %v ", iotPayload.Parms)
		respBuff.Write(
			[]byte(
				fmt.Sprintf(`{"retcode":0,"message":"command: skip %v"}`, iotPayload.Parms)))

		return
	}

	if iot.LogCode("mvc") || iot.LogNode(node) {

		iot.Info.Printf("mvcData-%s", iotPayload.Parms[1])
		iot.Detail.Print(respBuff.String())
	}

}

func mvcDataGlobal(respBuff *bytes.Buffer) {
	// var err error

	respBuff.Write([]byte(fmt.Sprintf(`{"mvcdata3":{"log_From":["%d"]`, 0)))

	// mvc3(conn, "s2__10", "0", 0)
	// iot.PropMvc(respBuff, iot.GetProp(2, 10), true)

	strTime := fmt.Sprintf("%02d:%02d", time.Now().Hour(), time.Now().Minute())
	if iot.TimerMode.Val > 0 {
		strTime += fmt.Sprintf(":%02d", time.Now().Second())
	}
	respBuff.Write([]byte(fmt.Sprintf(`,"svcTimer":["%s",%d]`, strTime, 1)))

	for nodeId, node := range iot.Nodes {

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

		for _, prop := range node.Props {
			if prop.GlobalMvc {
				//fmt.Println("mvcDataGlobal varID:", varID, "prop:", prop)
				iot.PropMvc(respBuff, prop, true)
			}
		}

		// fmt.Println("k:", nodeId, "v:", node)
	}

	respBuff.Write([]byte("}}"))

	// conn.Write([]byte(fmt.Sprintf(`],"count":%d,"start":%d,"stop":%d,"stopval":%d}`, cnt, startTimestamp, timestamp, val)))

	// fmt.Printf(`timedata: ],"count":%d,"start":%d,"stop":%d,"stopval":%d}\n\n`, cnt, startTimestamp, timestamp, val)

	respBuff.Write([]byte("\n"))
}
