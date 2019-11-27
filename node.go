package main

import (
	"iot"
	"strconv"
	"time"
)

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

func newNode(nodeID int) *iot.IotNode {

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
