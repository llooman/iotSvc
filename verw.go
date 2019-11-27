package main

import (
	"fmt"
	"time"
)

var AppName string
var actief bool
var power int
var dutyCycle int
var thuisByUser bool
var thuisByAgenda bool
var verwarmingNextActivity int64
var verwActive bool

func initVerw() {
	AppName = "verw"
	actief = false
	power = 0
	dutyCycle = 0

	thuisByUser = false
	thuisByAgenda = false
	verwActive = false
}

func verwarming() {

	verwActive = true
	verwarmingNextActivity = time.Now().Unix() + 3

	if verwActive {
		if time.Now().Unix() > verwarmingNextActivity {

			verwarmingNextActivity = time.Now().Unix() + int64(60)

			fmt.Printf("loopVerw:%s\n ", AppName)

		}

		time.Sleep(3 * time.Second)
	}

}
