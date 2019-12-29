package main

/*
	*     * * 10 * *
	+-------------- month (1 - 12)
	|  +----------- day in month (1 - 31)
	|  |  +-------- day in week (0 - 6) (Sunday=6)
	|  |  |  +----- hour (0 - 23)
	|  |  |  |  +-- min (0 - 59)
	|  |  |  |  |
	0  1  2  3  4
	*  *  *  *  *        command to be executed
// '12 *  *  23 55'
// '*  *  *  23 55'
// '*  *  *  23 55'
// '*  *  *  *  30'
// '*  *  *  9  10'
*/

import (
	"iot"
	"log"
	"strconv"
	"strings"
	"time"
)

type iotSchedule struct {
	name        string
	schedule    string
	action      string
	enabled     bool
	execOnStart bool
	next        int64
}

var schedulerRunning bool
var nextScheduleRun int64

// var prevScheduleRun int64
// prevScheduleRun := time.Now().Unix()

func scheduler() {

	var schedule iotSchedule

	schedulerRunning = true
	nextScheduleRun = 0

	next5Minute := int64(0)

	iot.Info.Printf("scheduler: Start\n")

	for schedulerRunning {

		// optimizeLogMem every 5 minutes
		// logMem2Disk every hour
		if next5Minute < time.Now().Unix() &&
			time.Now().Minute()%5 == 0 {
			optimizeLogMem()

			if time.Now().Minute() < 1 {
				_, err := iot.DatabaseNew.Exec("call logMem2Disk()")

				if err != nil {
					iot.Err.Printf("logMem2Disk err:%v", err.Error())
				}
			}
			next5Minute = time.Now().Unix() + 240
			// fmt.Printf("next5Minute:%d", next5Minute)
		}

		if time.Now().Unix() >= nextScheduleRun {

			nextScheduleRun = time.Now().Unix() + 15

			// fmt.Printf("------- scheduler -------\n")
			// fmt.Printf("kamperTemp:%f, ketelSchakelaar:%t, pompTemp:%d\n", kamerTemp, iot.KetelSchakelaar.Val > 0, iot.PompTemp.Val)

			var err error

			stmt, errr := iot.DatabaseNew.Prepare(`
			Select name, schedule, action, enabled, execOnStart, next
			from schedules where enabled = 1 and next <= ?  order by next`)

			checkError(errr)

			defer stmt.Close()

			rows, errr := stmt.Query(time.Now().Unix()) //

			if errr != nil {
				log.Fatal(err)
			}

			defer rows.Close()

			cnt := 0

			// var checkActionSql sql.NullString
			// var attempts int
			// var maxAttempts int
			// var periode int
			var enabled int
			var execOnStart int
			// var timestamp int64

			for rows.Next() {

				err := rows.Scan(&schedule.name, &schedule.schedule, &schedule.action, &enabled, &execOnStart, &schedule.next)
				schedule.enabled = enabled > 0
				schedule.execOnStart = execOnStart > 0

				checkError(err)

				cnt++
				//fmt.Printf("%s %s action %s \n", schedule.name, schedule.schedule, schedule.action)

				payload := iot.ToPayload(schedule.action)

				if schedule.enabled &&
					(schedule.execOnStart || schedule.next > 0) {
					payload.Timestamp = 0
					//fmt.Printf("payload %v \n", payload)
					command(&payload)
					nextSchedule(&schedule)
					iot.Trace.Printf("%s next:%s\n", schedule.name, fmtTime(schedule.next))
				} else {
					nextSchedule(&schedule)
					iot.Trace.Printf("%s first:%s\n", schedule.name, fmtTime(schedule.next))
				}

				persistSchedule(&schedule)

				//fmt.Printf("%s next:%s\n", schedule.name, fmtTime(schedule.next))

				if schedule.next > 0 &&
					schedule.next < nextScheduleRun {
					nextScheduleRun = schedule.next
				}

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

			// prevScheduleRun = time.Now().Unix()
		}

		time.Sleep(999 * time.Millisecond)
	}

	iot.Trace.Printf("scheduler loop Finished ???\n ")
}

func fmtTime(stamp int64) string {

	tm := time.Unix(stamp, 0)
	return tm.String()
}

// NextSchedule ...
func nextSchedule(schedule *iotSchedule) {

	nextDate := time.Now()

	var monthInYear string
	var dayInMonth string
	var dayInWeek string
	var hourInDay string
	var minuteInHour string
	var seconds string

	// fmt.Printf("nextSchedule: %s ", schedule)
	// fmt.Println(nextDate)

	scheduleItems := strings.Split(schedule.schedule, " ")

	if len(scheduleItems) >= 5 {
		monthInYear = scheduleItems[0]  // month (1 - 12)
		dayInMonth = scheduleItems[1]   // day of month (1 - 31)
		dayInWeek = scheduleItems[2]    // day of week (1 - 7) (Sunday=1)
		hourInDay = scheduleItems[3]    // hour (0 - 23)
		minuteInHour = scheduleItems[4] // minute (0 - 59)

		truncSeconds := 1 * time.Minute
		nextDate = time.Now().Truncate(truncSeconds).Add(time.Minute * time.Duration(1))

	} else if len(scheduleItems) >= 1 &&
		len(scheduleItems) < 3 {
		seconds = scheduleItems[0] // seconds (1 - 99999)

	} else {
		iot.Trace.Printf("Schedule err: %s.%s \n", schedule.name, schedule.schedule)
		schedule.next = 0
		return
	}

	newTime := false

	// fmt.Printf("nextSchedule:       month=%d        day=%d   WeekDay=%d \n", int(nextDate.Month()), nextDate.Day(), int(nextDate.Weekday()))
	// fmt.Printf("nextSchedule: monthInYear=%s dayInMonth=%s dayInWeek=%s hourInDay=%s minuteInHour=%s\n", monthInYear, dayInMonth, dayInWeek, hourInDay, minuteInHour)

	if scheduleMinute, err := strconv.Atoi(minuteInHour); err == nil {
		deltaMinute := (60 + scheduleMinute - nextDate.Minute()) % 60
		// fmt.Printf("deltaMinute:%d    \n", deltaMinute)
		nextDate = nextDate.Add(time.Minute * time.Duration(deltaMinute))
		newTime = true
	}

	if scheduleHour, err := strconv.Atoi(hourInDay); err == nil {
		deltaHour := (24 + scheduleHour - nextDate.Hour()) % 24
		// fmt.Printf("deltaHour:%d    \n", deltaHour)
		nextDate = nextDate.Add(time.Hour * time.Duration(deltaHour))
		newTime = true
	}

	if scheduleDay, err := strconv.Atoi(dayInMonth); err == nil {
		deltaDays := scheduleDay - nextDate.Day()
		// fmt.Printf("deltaDays:%d    \n", deltaDays)
		if deltaDays < 0 {
			nextDate = nextDate.AddDate(0, 1, 0)
			nextDate = time.Date(nextDate.Year(), time.Month(int(nextDate.Month())), scheduleDay, nextDate.Hour(), nextDate.Minute(), 0, 0, time.Now().Location())

		} else if deltaDays > 0 {
			nextDate = nextDate.AddDate(0, 0, deltaDays)

		}
		newTime = true
	}

	if scheduleWeekDay, err := strconv.Atoi(dayInWeek); err == nil {
		deltaWeekDays := (7 + scheduleWeekDay - int(nextDate.Weekday())) % 7
		// fmt.Printf("deltaWeekDays:%d    \n", deltaWeekDays)
		nextDate = nextDate.AddDate(0, 0, deltaWeekDays)
		newTime = true
	}

	if scheduleMonth, err := strconv.Atoi(monthInYear); err == nil {
		deltaMonths := (12 + scheduleMonth - int(nextDate.Month())) % 12
		// fmt.Printf("deltaMonths:%d    \n", deltaMonths)
		nextDate = nextDate.AddDate(0, deltaMonths, 0)
		newTime = true
	}

	if scheduleSeconds, err := strconv.Atoi(seconds); err == nil {
		// fmt.Printf("seconds:%d    \n", scheduleSeconds)
		nextDate = nextDate.Add(time.Second * time.Duration(scheduleSeconds))
		newTime = true
	}

	if !newTime {
		iot.Err.Printf("NextSchedule err: %s no schedule found %s \n", schedule.name, schedule.schedule)
		schedule.next = 0
		return
	}

	schedule.next = nextDate.Unix()
}

func persistSchedule(schedule *iotSchedule) {

	persist, err := iot.DatabaseNew.Prepare(`
	INSERT INTO schedules( name,
		schedule,
		action, 
		enabled, 
		execOnStart, 
		next ) VALUES(?, ?, ?, ?, ?, ?) 
	ON DUPLICATE KEY UPDATE schedule=?, 
		action=?, 
		enabled=?, 
		execOnStart=?, 
		next=? `)

	if err != nil {
		iot.Err.Printf("PersistSchedule: %v", err.Error())
	}

	defer persist.Close()

	_, err = persist.Exec(
		schedule.name,
		schedule.schedule,
		schedule.action,
		schedule.enabled,
		schedule.execOnStart,
		schedule.next,
		schedule.schedule,
		schedule.action,
		schedule.enabled,
		schedule.execOnStart,
		schedule.next)

	if err != nil {
		iot.Err.Printf("PersistSchedule: %v", err.Error())
	}
}
