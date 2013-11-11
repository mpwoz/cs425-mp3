package ring

import (
	"../data"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

func (self *Ring) createTCPListener(hostPort string) {
	var tcpaddr *net.TCPAddr
	tcpaddr, err := net.ResolveTCPAddr("tcp", hostPort)
	if err != nil {
		return
	}
	rpc.Register(self)
	rpc.HandleHTTP()

	conn, err := net.ListenTCP("tcp", tcpaddr)
	if err != nil {
		log.Fatal("listen error:", err)
	}
	go http.Serve(conn, nil)
}


/*
   The following are the calls exposed over RPC and may be called from a remote machine
   They correspont to the Insert/Update/Remove/Lookup calls on the client
*/

/* Insert */
func (self *Ring) SendData(sentData *data.DataStore, response *RpcResult) error {
	inserted := self.KeyValTable.Insert(data.DataStore{(*sentData).Key, (*sentData).Value})
	response.Success = Btoi(inserted)
	return nil
}

/* Remove */
func (self *Ring) RemoveData(args *data.DataStore, response *RpcResult) error {
	deleted := self.KeyValTable.DeleteWithKey(data.DataStore{(*args).Key, ""})
	response.Success = Btoi(deleted)
	return nil
}

/* Lookup */
func (self *Ring) GetData(args *data.DataStore, response *RpcResult) error {
	found := self.KeyValTable.Get(data.DataStore{(*args).Key, ""})
	if found == nil {
		fmt.Println("Data not found")
		response.Success = 0
	} else {
    response.Success = 1
		response.Data = found.(data.DataStore)
	}
	return nil
}

/* Update : Delete the current data, then add the new */
func (self *Ring) UpdateData(sentData *data.DataStore, response *RpcResult) error {
	self.KeyValTable.DeleteWithKey(data.DataStore{(*sentData).Key, ""})
	inserted := self.KeyValTable.Insert(data.DataStore{(*sentData).Key, (*sentData).Value})
	response.Success = Btoi(inserted)
	return nil
}

//Get data for when joining the group
func (self *Ring) GetEntryData(key *int, responseData *data.DataStore) error {

	mdata := &data.DataStore{
		Key:   -1,
		Value: "",
	}

	*responseData = *mdata

	False := self.KeyValTable.Limit()
	min := self.KeyValTable.Min()

	if min != False {
		if min.Item().(data.DataStore).Key <= *key {
			*responseData = min.Item().(data.DataStore)
			self.KeyValTable.DeleteWithIterator(min)
		}
	}
	return nil
}

/*
  Some other utility functions that may be called over RPC
*/
func (self *Ring) GetSuccessor(key *int, currSuccessorMember **data.GroupMember) error {

	start := self.UserKeyTable.Min()
	for i := 0; i < self.UserKeyTable.Len(); i++ {
		value := start.Item().(locationStore).value
		fmt.Println(self.Usertable[value])
		start = start.Next()
	}

	successorItem := self.UserKeyTable.FindGE(locationStore{*key + 1, ""})
	overFlow := self.UserKeyTable.Limit()
	fmt.Println(successorItem)
	if successorItem == overFlow {
		fmt.Println("overflow")
		successorItem = self.UserKeyTable.Min()
	}
	if successorItem != self.UserKeyTable.Limit() {
		fmt.Println("IGetting")
		item := successorItem.Item()
		value := item.(locationStore).value
		member := self.Usertable[value]
		fmt.Println(member.Id)
		*currSuccessorMember = member
		//We can add code to update member key here as well? Or we can wait for it to be gossiped to us

	} else {
		*currSuccessorMember = nil
	}
	return nil
}

// Utility bool-to-int conversion
func Btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}
