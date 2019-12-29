package main

import (
	"iot"
	"strconv"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func initMq() {

	mqttClient = iot.NewMQClient(iotConfig.MqttUri, iotConfig.ServerId+"iotSvc")
	mqttClient3b = iot.NewMQClient(iotConfig.MqttUri3b, iotConfig.ServerId+"iotSvc")

	iot.MqttClient3b = mqttClient3b
	iot.MqttClient3 = mqttClient

	go mqListenUp(iotConfig.MqttUp)
	go mqListenUp3b(iotConfig.MqttUp)

	// go mqListenCmd(iotConfig.MqttCmd)

}

// func mqListenCmd(topic string) {

// 	fmt.Printf("listen: %s  \n", topic)

// 	// client := mqConnect("iotCmd", uri)
// 	client := iot.NewMQClient(iotConfig.MqttUri, iotConfig.ServerId+"iotCmd")

// 	client.Subscribe(topic, 0, func(client mqtt.Client, msg mqtt.Message) {

// 		var payload = string(msg.Payload())
// 		// fmt.Printf("[%s]>%s\n", topic, payload)
// 		iotPayload := iot.ToPayload(payload)

// 		//command(&iotPayload)

// 		fmt.Printf("iotCmd: cmd %s n%dp%d ConnId %d Val %d Timestamp %d\n", iotPayload.Cmd, iotPayload.NodeId, iotPayload.PropId, iotPayload.ConnId, iotPayload.Val, iotPayload.Timestamp)

// 		// TODO update timestamp conn node so we know it is still alive

// 	})
// }

func mqListenUp3b(topic string) {

	iot.Info.Printf("Start %s on 3b  \n", topic)

	client := iot.NewMQClient(iotConfig.MqttUri3b, iotConfig.ServerId+"iotUp")

	client.Subscribe(topic, 0, func(client mqtt.Client, msg mqtt.Message) {

		var payload = string(msg.Payload())

		if len(payload) >= 0 {

			// payloadArr := strings.Split(payloadBuf, "\n")
			// for _, payload := range payloadArr {

			// if iot.LogDebug {
			// 	iot.Trace.Printf("Up3b %s [%s]", payload, topic)
			// }

			iotPayload := iot.ToPayload(payload)

			if iot.LogPayload(&iotPayload) {
				iot.Trace.Printf("Up3b %s [%s] %v\n", payload, topic, iotPayload)
			}

			if iotPayload.Cmd == "u" || iotPayload.Cmd == "U" {

				prop := iot.GetProp(iotPayload.NodeId, iotPayload.PropId)
				iot.CheckConnId(prop.Node, iotPayload.ConnId)
				iot.SaveStrProp(prop, iotPayload.Val, iotPayload.Timestamp)

			} else if iotPayload.Cmd == "e" || iotPayload.Cmd == "E" {

				prop := iot.GetProp(iotPayload.NodeId, iotPayload.PropId)
				iot.CheckConnId(prop.Node, iotPayload.ConnId)
				prop.IsDataSaved = false
				// prop.IsNew = false
				prop.Err = iotPayload.Val
				prop.InErr = true
				prop.ErrStamp = time.Now().Unix()
				prop.Node.Timestamp = time.Now().Unix()

				iot.PersistPropToMem(prop)

				// } else if iotPayload.Cmd == "S" || iotPayload.Cmd == "s" {
				// should be gone when cmd queue is operational

			} else if iotPayload.Cmd == "D" || iotPayload.Cmd == "d" {

				prop := iot.GetProp(iotPayload.NodeId, iotPayload.PropId)
				if prop != nil {
					if prop.IsStr {
						iot.SendProp(prop, "S", prop.ValString)
					} else {
						iot.SendProp(prop, "S", strconv.FormatInt(prop.Val, 10))
					}
				}

			} else if iotPayload.Cmd == "R" &&
				iotPayload.NodeId == 1 &&
				iotPayload.PropId == 10 {

				connProp := iot.GetProp(1, 10)
				if connProp != nil {
					iot.SendProp(connProp, "S", iot.LogLevel.ValString)
				}

			} else {
				iot.Info.Printf("mqUp.skip %s [%s]\n", payload, msg.Topic())

			}
		}
	})
}

func mqListenUp(topic string) {

	iot.Info.Printf("Start %s on old 3\n", topic)

	client := iot.NewMQClient(iotConfig.MqttUri, iotConfig.ServerId+"iotUp")

	client.Subscribe(topic, 0, func(client mqtt.Client, msg mqtt.Message) {

		var payload = string(msg.Payload())

		iotPayload := iot.ToPayload(payload)

		if iot.LogPayload(&iotPayload) {
			iot.Trace.Printf("Up3b %s [%s] %v\n", payload, topic, iotPayload)
		}

		if iotPayload.Cmd == "u" || iotPayload.Cmd == "U" {

			prop := iot.GetProp(iotPayload.NodeId, iotPayload.PropId)
			iot.CheckConnId(prop.Node, iotPayload.ConnId)
			iot.SaveStrProp(prop, iotPayload.Val, iotPayload.Timestamp)

		} else if iotPayload.Cmd == "e" || iotPayload.Cmd == "E" {

			prop := iot.GetProp(iotPayload.NodeId, iotPayload.PropId)
			iot.CheckConnId(prop.Node, iotPayload.ConnId)
			prop.IsDataSaved = false
			// prop.IsNew = false
			prop.Err = iotPayload.Val
			prop.InErr = true
			prop.ErrStamp = time.Now().Unix()
			prop.Node.Timestamp = time.Now().Unix()

			iot.PersistPropToMem(prop)

		} else {
			iot.Err.Printf("mqUp.skip %s [%s]\n", payload, msg.Topic())

		}
	})
}

// func updateConnId(prop *iot.IotProp, connId int) {

// 	if prop.Node.ConnId != connId &&
// 		connId > 0 {

// 		prop.Node.ConnId = connId
// 		prop.Node.IsDefSaved = false
// 		iot.PersistNodeDef(prop.Node)

// 		fmt.Printf("updateConnId node:%d to %d\n", prop.Node.NodeId, connId)
// 	}
// }
