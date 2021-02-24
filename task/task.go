// Copyright 2014 beego Author. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package task

import (
	"context"
	"github.com/robfig/cron/v3"
	"log"
	"sort"
	"sync"
	"time"
	//"github.com/robfig/cron/v3"
)

type taskManager struct {
	adminTaskList map[string]Tasker
	taskLock      sync.RWMutex
	stop          chan bool
	changed       chan bool
	started       bool
}

func newTaskManager() *taskManager {
	return &taskManager{
		adminTaskList: make(map[string]Tasker),
		taskLock:      sync.RWMutex{},
		stop:          make(chan bool),
		changed:       make(chan bool),
		started:       false,
	}
}

var (
	globalTaskManager *taskManager
)

const (
	// Set the top bit if a star was included in the expression.
	starBit = 1 << 63
)

// Schedule time taks schedule
type Schedule struct {
	Second uint64
	Minute uint64
	Hour   uint64
	Day    uint64
	Month  uint64
	Week   uint64
}

// TaskFunc task func type
type TaskFunc func(ctx context.Context) error

// Tasker task interface
type Tasker interface {
	GetSpec(ctx context.Context) string
	GetStatus(ctx context.Context) string
	Run(ctx context.Context) error
	SetNext(context.Context, time.Time)
	GetNext(ctx context.Context) time.Time
	SetPrev(context.Context, time.Time)
	GetPrev(ctx context.Context) time.Time
	GetTimeout(ctx context.Context) time.Duration
}

// task error
type taskerr struct {
	t       time.Time
	errinfo string
}

// Task task struct
// It's not a thread-safe structure.
// Only nearest errors will be saved in ErrList
type Task struct {
	Taskname string
	Spec     *Schedule
	SpecStr  string
	DoFunc   TaskFunc
	Prev     time.Time
	Next     time.Time
	Timeout  time.Duration // timeout duration
	Errlist  []*taskerr    // like errtime:errinfo
	ErrLimit int           // max length for the errlist, 0 stand for no limit
	errCnt   int           // records the error count during the execution
	c        *cron.Cron    // cron
	id       cron.EntryID  // task id
}

// NewTask add new task with name, time and func
func NewTask(tname string, spec string, f TaskFunc, opts ...Option) *Task {

	task := &Task{
		Taskname: tname,
		DoFunc:   f,
		// Make configurable
		ErrLimit: 100,
		SpecStr:  spec,
		// we only store the pointer, so it won't use too many space
		Errlist: make([]*taskerr, 100, 100),
	}

	for _, opt := range opts {
		opt.apply(task)
	}

	task.SetCron(spec)
	return task
}

// GetSpec get spec string
func (t *Task) GetSpec(context.Context) string {
	return t.SpecStr
}

// GetStatus get current task status
func (t *Task) GetStatus(context.Context) string {
	var str string
	for _, v := range t.Errlist {
		if v == nil {
			continue
		}
		str += v.t.String() + ":" + v.errinfo + "<br>"
	}
	return str
}

// Run run all tasks
func (t *Task) Run(ctx context.Context) error {
	err := t.DoFunc(ctx)
	if err != nil {
		index := t.errCnt % t.ErrLimit
		t.Errlist[index] = &taskerr{t: t.Next, errinfo: err.Error()}
		t.errCnt++
	}
	return err
}

// SetNext set next time for this task
func (t *Task) SetNext(ctx context.Context, now time.Time) {
}

// GetNext get the next call time of this task
func (t *Task) GetNext(context.Context) time.Time {
	if t.c != nil {
		return t.c.Entry(t.id).Next
	}

	return time.Time{}
}

// SetPrev set prev time of this task
func (t *Task) SetPrev(ctx context.Context, now time.Time) {
	t.Prev = now
}

// GetPrev get prev time of this task
func (t *Task) GetPrev(context.Context) time.Time {
	return t.Prev
}

// GetTimeout get timeout duration of this task
func (t *Task) GetTimeout(context.Context) time.Duration {
	return t.Timeout
}

// Option interface
type Option interface {
	apply(*Task)
}

// optionFunc return a function to set task element
type optionFunc func(*Task)

// apply option to task
func (f optionFunc) apply(t *Task) {
	f(t)
}

// TimeoutOption return a option to set timeout duration for task
func TimeoutOption(timeout time.Duration) Option {
	return optionFunc(func(t *Task) {
		t.Timeout = timeout
	})
}

// six columns mean：
//       second：0-59
//       minute：0-59
//       hour：1-23
//       day：1-31
//       month：1-12
//       week：0-6（0 means Sunday）

// SetCron some signals：
//       *： any time
//       ,：　 separate signal
// 　　    －：duration
//       /n : do as n times of time duration
// ///////////////////////////////////////////////////////
//	0/30 * * * * *                        every 30s
//	0 43 21 * * *                         21:43
//	0 15 05 * * * 　　                     05:15
//	0 0 17 * * *                          17:00
//	0 0 17 * * 1                           17:00 in every Monday
//	0 0,10 17 * * 0,2,3                   17:00 and 17:10 in every Sunday, Tuesday and Wednesday
//	0 0-10 17 1 * *                       17:00 to 17:10 in 1 min duration each time on the first day of month
//	0 0 0 1,15 * 1                        0:00 on the 1st day and 15th day of month
//	0 42 4 1 * * 　 　                     4:42 on the 1st day of month
//	0 0 21 * * 1-6　　                     21:00 from Monday to Saturday
//	0 0,10,20,30,40,50 * * * *　           every 10 min duration
//	0 */10 * * * * 　　　　　　              every 10 min duration
//	0 * 1 * * *　　　　　　　　               1:00 to 1:59 in 1 min duration each time
//	0 0 1 * * *　　　　　　　　               1:00
//	0 0 */1 * * *　　　　　　　               0 min of hour in 1 hour duration
//	0 0 * * * *　　　　　　　　               0 min of hour in 1 hour duration
//	0 2 8-20/3 * * *　　　　　　             8:02, 11:02, 14:02, 17:02, 20:02
//	0 30 5 1,15 * *　　　　　　              5:30 on the 1st day and 15th day of month
func (t *Task) SetCron(spec string) {
	t.Spec = t.parse(spec)
}

func (t *Task) parse(spec string) *Schedule {
	if len(spec) > 0 && spec[0] == '@' {
		return t.parseSpec(spec)
	}

	schedule := &Schedule{}

	return schedule
}

func (t *Task) parseSpec(spec string) *Schedule {
	switch spec {
	case "@yearly", "@annually":
		t.SpecStr = "0 0 0 1 1 *"
	case "@monthly":
		t.SpecStr = "0 0 0 1 * *"
	case "@weekly":
		t.SpecStr = "0 0 0 * * 0"
	case "@daily", "@midnight":
		t.SpecStr = "0 0 0 * * *"
	case "@hourly":
		t.SpecStr = "0 0 * * * *"
	default:
		log.Panicf("Unrecognized descriptor: %s", spec)
	}
	return &Schedule{}
}

// Next set schedule to next time
func (s *Schedule) Next(t time.Time) time.Time {
	return t
}

func dayMatches(s *Schedule, t time.Time) bool {
	var (
		domMatch = 1<<uint(t.Day())&s.Day > 0
		dowMatch = 1<<uint(t.Weekday())&s.Week > 0
	)

	if s.Day&starBit > 0 || s.Week&starBit > 0 {
		return domMatch && dowMatch
	}
	return domMatch || dowMatch
}

// StartTask start all tasks
func StartTask() {
	globalTaskManager.StartTask()
}

// StopTask stop all tasks
func StopTask() {
	globalTaskManager.StopTask()
}

// AddTask add task with name
func AddTask(taskName string, t Tasker) {
	globalTaskManager.AddTask(taskName, t)
}

// DeleteTask delete task with name
func DeleteTask(taskName string) {
	globalTaskManager.DeleteTask(taskName)
}

//  ClearTask clear all tasks
func ClearTask() {
	globalTaskManager.ClearTask()
}

// StartTask start all tasks
func (m *taskManager) StartTask() {
	m.taskLock.Lock()
	defer m.taskLock.Unlock()
	if m.started {
		// If already started， no need to start another goroutine.
		return
	}
	m.started = true

	registerCommands()
	go m.run()
}

func (m *taskManager) run() {
	now := time.Now().Local()
	// first run the tasks, so set all tasks next run time.
	//m.setTasksStartTime(now)

	for {
		// we only use RLock here because NewMapSorter copy the reference, do not change any thing
		// here, we sort all task and get first task running time (effective).
		m.taskLock.RLock()
		sortList := NewMapSorter(m.adminTaskList)
		m.taskLock.RUnlock()
		sortList.Sort()
		var effective time.Time
		if len(m.adminTaskList) == 0 || sortList.Vals[0].GetNext(context.Background()).IsZero() {
			// If there are no entries yet, just sleep - it still handles new entries
			// and stop requests.
			effective = now.AddDate(10, 0, 0)
		} else {
			effective = sortList.Vals[0].GetNext(context.Background())
		}

		select {
		case now = <-time.After(effective.Sub(now)): // wait for effective time
			runNextTasks(sortList, effective)
			continue
		case <-m.changed: // tasks have been changed, set all tasks run again now
			now = time.Now().Local()
			//m.setTasksStartTime(now)
			continue
		case <-m.stop: // manager is stopped, and mark manager is stopped
			m.markManagerStop()
			return
		}
	}
}

// markManagerStop it sets manager to be stopped
func (m *taskManager) markManagerStop() {
	m.taskLock.Lock()
	if m.started {
		m.started = false
	}
	m.taskLock.Unlock()
}

// runNextTasks it runs next task which next run time is equal to effective
func runNextTasks(sortList *MapSorter, effective time.Time) {
	// Run every entry whose next time was this effective time.
	var i = 0
	for _, e := range sortList.Vals {
		i++
		if e.GetNext(context.Background()) != effective {
			break
		}

		// check if timeout is on, if yes passing the timeout context
		ctx := context.Background()
		if duration := e.GetTimeout(ctx); duration != 0 {
			go func(e Tasker) {
				ctx, cancelFunc := context.WithTimeout(ctx, duration)
				defer cancelFunc()
				err := e.Run(ctx)
				if err != nil {
					log.Printf("tasker.run err: %s\n", err.Error())
				}
			}(e)
		} else {
			go func(e Tasker) {
				err := e.Run(ctx)
				if err != nil {
					log.Printf("tasker.run err: %s\n", err.Error())
				}
			}(e)
		}

		e.SetPrev(context.Background(), e.GetNext(context.Background()))
		e.SetNext(context.Background(), effective)
	}
}

// StopTask stop all tasks
func (m *taskManager) StopTask() {
	go func() {
		m.stop <- true
	}()
}

// AddTask add task with name
func (m *taskManager) AddTask(taskname string, t Tasker) {
	isChanged := false
	m.taskLock.Lock()
	t.SetNext(nil, time.Now().Local())
	m.adminTaskList[taskname] = t
	if m.started {
		isChanged = true
	}
	m.taskLock.Unlock()

	if isChanged {
		go func() {
			m.changed <- true
		}()
	}

}

// DeleteTask delete task with name
func (m *taskManager) DeleteTask(taskname string) {
	isChanged := false

	m.taskLock.Lock()
	delete(m.adminTaskList, taskname)
	if m.started {
		isChanged = true
	}
	m.taskLock.Unlock()

	if isChanged {
		go func() {
			m.changed <- true
		}()
	}
}

//  ClearTask clear all tasks
func (m *taskManager) ClearTask() {
	isChanged := false

	m.taskLock.Lock()
	m.adminTaskList = make(map[string]Tasker)
	if m.started {
		isChanged = true
	}
	m.taskLock.Unlock()

	if isChanged {
		go func() {
			m.changed <- true
		}()
	}
}

// MapSorter sort map for tasker
type MapSorter struct {
	Keys []string
	Vals []Tasker
}

// NewMapSorter create new tasker map
func NewMapSorter(m map[string]Tasker) *MapSorter {
	ms := &MapSorter{
		Keys: make([]string, 0, len(m)),
		Vals: make([]Tasker, 0, len(m)),
	}
	for k, v := range m {
		ms.Keys = append(ms.Keys, k)
		ms.Vals = append(ms.Vals, v)
	}
	return ms
}

// Sort sort tasker map
func (ms *MapSorter) Sort() {
	sort.Sort(ms)
}

func (ms *MapSorter) Len() int { return len(ms.Keys) }
func (ms *MapSorter) Less(i, j int) bool {
	if ms.Vals[i].GetNext(context.Background()).IsZero() {
		return false
	}
	if ms.Vals[j].GetNext(context.Background()).IsZero() {
		return true
	}
	return ms.Vals[i].GetNext(context.Background()).Before(ms.Vals[j].GetNext(context.Background()))
}
func (ms *MapSorter) Swap(i, j int) {
	ms.Vals[i], ms.Vals[j] = ms.Vals[j], ms.Vals[i]
	ms.Keys[i], ms.Keys[j] = ms.Keys[j], ms.Keys[i]
}

func init() {
	globalTaskManager = newTaskManager()
}
