/*

@author liujiarik
*/
package safemap

import (
	"bytes"
	"encoding/gob"
)

const (
	remove   commandAction = iota
	get
	put
	length
	snapshot
	update
	end
)

type SafeMap struct {
	commandStream chan command
}

type command struct {
	action           commandAction
	key              string
	value            interface{}
	res              chan interface{}
	getCallBack      func(interface{}, bool)
	lenCallBack      func(int)
	updateCallBack   func(interface{})
	atomicUpdateFunc updater
}
// updater is function to define how to atomic update a map entry
// the first arg is the value of the entry.
// if value is not empty ,the second bool arg will be true.
//
type updater func(interface{}, bool) (interface{})

type commandAction int

type getResult struct {
	value interface{}
	exist bool
}

// create new SafeMap
func New() *SafeMap {

	sm := &SafeMap{}
	sm.commandStream = make(chan command)
	go sm.run()
	return sm
}

// put a entry to map
func (sm *SafeMap) Put(key string, value interface{}) {
	sm.commandStream <- command{action: put, key: key, value: value}
}

// remove a entry form map
func (sm *SafeMap) Remove(key string) {
	sm.commandStream <- command{action: remove, key: key}
}

// get a entry form map synchronously
func (sm *SafeMap) Get(key string) (interface{}, bool) {

	reply := make(chan interface{})
	sm.commandStream <- command{action: get, key: key, res: reply}
	res := (<-reply).(getResult)
	return res.value, res.exist
}

// get the length of map synchronously
func (sm *SafeMap) Len() (int) {

	reply := make(chan interface{})
	sm.commandStream <- command{action: length, res: reply}
	return (<-reply).(int)
}

// close the safeMap.
// after closing ,any action will throw a panic ,
// and the function will return a map set
func (sm *SafeMap) Close() (map[string]interface{}) {

	reply := make(chan interface{})
	sm.commandStream <- command{action: end, res: reply}
	return (<-reply).(map[string]interface{})
}

//atomic update a entry by updater
func (sm *SafeMap) Update(key string, updateFunc updater) (interface{}) {

	reply := make(chan interface{})
	sm.commandStream <- command{action: update, res: reply, atomicUpdateFunc: updateFunc, key: key}
	return <-reply
}

func (sm *SafeMap) Snapshot() (map[string]interface{}) {

	reply := make(chan interface{})
	sm.commandStream <- command{action: snapshot, res: reply}
	return (<-reply).(map[string]interface{})
}

func (sm *SafeMap) AsyncGet(key string, callback func(interface{}, bool)) {

	sm.commandStream <- command{action: get, key: key, getCallBack: callback}

}
func (sm *SafeMap) AsyncLen(key string, callback func(int)) {
	sm.commandStream <- command{action: get, key: key, lenCallBack: callback}
}

func (sm *SafeMap) AsyncUpdate(key string, updateFunc updater, callback func(interface{})) {

	sm.commandStream <- command{action: update, atomicUpdateFunc: updateFunc, key: key, updateCallBack: callback}

}

//listen command Stream,and execute command in order
//
func (sm *SafeMap) run() {

	store := make(map[string]interface{}) // the places to save

	for c := range sm.commandStream {

		switch c.action {
		case remove:
			delete(store, c.key)
		case put:
			store[c.key] = c.value
		case get:
			v, f := store[c.key]
			if c.res != nil {
				c.res <- getResult{v, f}
			}
			if c.getCallBack != nil {
				c.getCallBack(v, f)
			}
		case length:
			if c.res != nil {
				c.res <- len(store)
			}
			if c.lenCallBack != nil {
				c.lenCallBack(len(store))
			}
		case update:
			v, f := store[c.key]
			store[c.key] = c.atomicUpdateFunc(v, f)
			if c.res != nil {
				c.res <- store[c.key]
			}
			if c.updateCallBack != nil {
				c.updateCallBack(store[c.key])
			}

		case snapshot:
			snapshotMap := make(map[string]interface{})
			deepCopy(snapshotMap, store)
			c.res <- snapshotMap
		case end:
			close(sm.commandStream)
			c.res <- store
		}

	}

}

func deepCopy(dst, src interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
}
