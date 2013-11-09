/*
  Represents the ring of machines. Hashing a machine or key to a value on the
  ring is done here.
*/

package ring

import (
	"../data"
	"../github.com/yasushi-saito/rbtree"
	"../logger"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"strings"
	"time"
)

type locationStore struct {
	key   int
	value string
}

const (
	Leaving = iota
	Stable
	Joining
)

type Ring struct {
	Usertable    map[string]*data.GroupMember
	UserKeyTable *rbtree.Tree
	KeyValTable  *rbtree.Tree
	Port         string
	Address      string
	Heartbeats   int
	ConnUDP      *net.UDPConn
	Active       bool
	isGossiping  bool
	Successor    *data.GroupMember
}

func NewMember(hostPort string, faultTolerance int) (ring *Ring, err error) {

	log.Printf("Creating udp listener at %s\n", hostPort)
	logger.Log("INFO", "Creating udp listener at"+hostPort)
	connUDP, err := createUDPListener(hostPort)

	delim := ":"
	fields := strings.SplitN(hostPort, delim, 2)
	address, port := fields[0], fields[1]

	if err != nil {
		return
	}

	userKeyVal := rbtree.NewTree(func(a, b rbtree.Item) int { return a.(locationStore).key - b.(locationStore).key })
	keyVal := rbtree.NewTree(func(a, b rbtree.Item) int { return a.(data.DataStore).Key - b.(data.DataStore).Key })

	ring = &Ring{
		Usertable:    make(map[string]*data.GroupMember),
		UserKeyTable: userKeyVal,
		KeyValTable:  keyVal,
		Port:         port,
		Address:      address,
		Heartbeats:   faultTolerance,
		ConnUDP:      connUDP,
		Active:       true,
		isGossiping:  false,
		Successor:    nil,
	}

	ring.createTCPListener(hostPort)
	fmt.Print(ring.Usertable)

	log.Println("UDP listener created!")
	logger.Log("INFO", "UDP listener Created")
	return

}

func (self *Ring) updateMember(value *data.GroupMember) {

	//	fmt.Println("Updating members")
	key := value.Id
	movement := value.Movement
	//fmt.Println(movement)
	//fmt.Println(value.Address)
	//fmt.Println(self.Usertable)
	//fmt.Println(self.UserKeyTable)
	member := self.Usertable[value.Address]
	var lastKey int
	if member != nil {
		lastKey = self.Usertable[value.Address].Id
	}

	//Delete member
	if key == -1 && movement == Leaving {
		fmt.Printf("Deleting member with ID %d", key)
		delete(self.Usertable, value.Address)
		//delete(self.UserKeyTable, lastKey)
		self.UserKeyTable.DeleteWithKey(lastKey)
		return

		//Add new member
	} else if member == nil {

		//fmt.Println("Adding new member to group")
		self.Usertable[value.Address] = value
		//if self.UserKeyTable[key] == "" {
		if self.UserKeyTable.Get(locationStore{key, ""}) == nil {
			self.UserKeyTable.Insert(locationStore{key, value.Address})

		} else {
			fmt.Println("ERROR: Two members with same key")
		}
		//fmt.Println(self.Usertable)
		//fmt.Println(self.UserKeyTable)
		return
		//Change current member key to new one
	} else {
		if member.Movement >= movement {
			self.Usertable[value.Address] = value
			if ((movement == Joining || member.Movement == Joining) && (key > lastKey)) ||
				((movement == Leaving || member.Movement == Leaving) && (key < lastKey)) {

				self.UserKeyTable.DeleteWithKey(lastKey)
				self.UserKeyTable.Insert(locationStore{key, value.Address})
			}
		} else {
			fmt.Println("You should not be able to join if you already exist or stay if you already started leaving")
		}

	}
	//Handle change in heartbeat

	return
}

func (self *Ring) FirstMember(portAddress string) {
	key := data.Hasher(portAddress)
	fmt.Println("Found")
	fmt.Println(key)
	newMember := data.NewGroupMember(key, portAddress, 0, Stable)
	self.updateMember(newMember)
}

func (self *Ring) GetSuccessor(key *int, currSuccessorMember **data.GroupMember) error {

	successorItem := self.UserKeyTable.FindGE(locationStore{*key, ""})
	if successorItem == self.UserKeyTable.Limit() {
		successorItem = self.UserKeyTable.Min()
	}
	if successorItem != self.UserKeyTable.Limit() {
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

func (self *Ring) GetData(key *int, responseData *data.DataStore) error {

	mdata := &data.DataStore{
		Key:   -1,
		Value: "",
	}

	*responseData = *mdata

	False := self.KeyValTable.Limit()
	min := self.KeyValTable.Min()

	if min != False {
		fmt.Println("******Should not happen")
		if min.Item().(locationStore).key <= *key {
			*responseData = min.Item().(data.DataStore)
			self.KeyValTable.DeleteWithIterator(min)
		}
	}
	return nil
}

func (self *Ring) SendData(sentData *data.DataStore, success *int) {

	*success = 0
	if self.KeyValTable.Insert(*sentData) {
		*success = 1
	}
}

func (self *Ring) Gossip() {
	fmt.Println("Start Gossiping")
	self.isGossiping = false
	heartbeatInterval := 50 * time.Millisecond
	userTableInterval := 2000 * time.Millisecond

	go self.HeartBeatGossip(heartbeatInterval)
	go self.UserTableGossip(userTableInterval)
}

func (self *Ring) HeartBeatGossip(interval time.Duration) {
	for {
		//self.doHeartBeatGossip
		time.Sleep(interval)
	}

}

func (self *Ring) UserTableGossip(interval time.Duration) {
	for {
		self.doUserTableGossip()
		time.Sleep(interval)
	}

}

func (self *Ring) ReceiveDatagrams(joinGroupOnConnection bool) {
	if self.Active == false {
		return
	}
	for {
		buffer := make([]byte, 1024)
		c, addr, err := self.ConnUDP.ReadFromUDP(buffer)
		if err != nil {
			log.Printf("%d byte datagram from %s with error %s\n", c, addr.String(), err.Error())
			logger.Log("ERROR", addr.String()+"byte datagram from %s with error "+err.Error())
			return
		}

		//log.Printf("Bytes received: %d\n", c)

		portmsg := strings.SplitN(string(buffer[:c]), "<PORT>", 2)
		port, msg := portmsg[0], portmsg[1]
		senderAddr := net.JoinHostPort(addr.IP.String(), port)

		//log.Printf("Data received from %s: %s", senderAddr, msg)
		logger.Log("INFO", "Data received from "+senderAddr+" : "+msg)

		self.handleMessage(msg, senderAddr, &joinGroupOnConnection)
	}
}

func (self *Ring) handleMessage(msg, sender string, joinSenderGroup *bool) {
	fields := strings.SplitN(msg, "|%|", 2)
	switch fields[0] {
	case "GOSSIP":
		logger.Log("GOSSIP", "Gossiping "+sender+fields[1])
		self.handleGossip(sender, fields[1])
	}
}

func (self *Ring) handleGossip(senderAddr, subject string) {
	// Reset the counter for the sender
	// TODO add sender if it doesn't exist yet

	subjectMember := data.Unmarshal(subject)
	if subjectMember == nil {
		return
	}

	self.updateMember(subjectMember)
	//fmt.Printf("My location Table Size: %d", self.UserKeyTable.Len())
	start := self.UserKeyTable.Min()
	for i := 0; i < self.UserKeyTable.Len(); i++ {
		//value := start.Item().(locationStore).value
		//fmt.Println(self.Usertable[value])
		start = start.Next()
	}
}

//Join the group by finding successor and getting all the required data from it
func (self *Ring) JoinGroup(address string) (err error) {

	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	//Get Successor
	hostPort := net.JoinHostPort(self.Address, self.Port)
	hashedKey := data.Hasher(hostPort)

	successor := self.callForSuccessor(hashedKey, address)
	argi := &hashedKey
	client, err = rpc.DialHTTP("tcp", successor.Address)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	//Get smallest key less then key and initiate data transfer
	var data_t data.DataStore
	err = client.Call("Ring.GetData", argi, &data_t)

	fmt.Printf("Transferring Data Key: %d Value: %s", data_t.Key, data_t.Value)
	for data_t.Key != -1 {

		//Insert Key into my table
		self.KeyValTable.Insert(locationStore{data_t.Key, data_t.Value})

		hostPort := net.JoinHostPort(self.Address, self.Port)

		//Insert value of key as my Id
		newMember := data.NewGroupMember(data_t.Key, hostPort, 0, Joining)
		self.updateMember(newMember)

		//Start Gossiping
		if self.isGossiping == false {
			go self.Gossip()
		}

		//Check if more data_t is available
		err = client.Call("Ring.GetData", argi, &data_t)
		if err != nil {
			fmt.Println("Error retrieving data")
			return
		}
	}
	//Make hashed key my id
	finalMember := data.NewGroupMember(hashedKey, hostPort, 0, Stable)
	self.updateMember(finalMember)

	if self.isGossiping == false {
		go self.Gossip()
	}
	return
}

//Leave the group by transferring all data to successor
func (self *Ring) LeaveGroup() {

	//TODO: We have stored successor but he could change so lets find ask a random member
	hostPort := net.JoinHostPort(self.Address, self.Port)
	key := self.Usertable[hostPort].Id
	receiver := self.getRandomMember()
	successor := self.callForSuccessor(key, receiver.Address)

	client, err := rpc.DialHTTP("tcp", successor.Address)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	NextLessThen := self.KeyValTable.FindLE(data.DataStore{key, ""})

	for NextLessThen != self.KeyValTable.NegativeLimit() {

		sendData := NextLessThen.Item().(data.DataStore)
		sendDataPtr := &sendData
		var result int
		err = client.Call("Ring.SendData", sendDataPtr, &result)
		if err != nil {
			fmt.Println("Error sending data")
			return
		}
		if result == 1 {
			fmt.Println("Data Succesfully sent")
			self.KeyValTable.DeleteWithIterator(NextLessThen)
			self.updateMember(data.NewGroupMember(sendData.Key, hostPort, 0, Leaving))
		} else {
			fmt.Println("Error sending data")
			break
		}
		NextLessThen = self.KeyValTable.FindLE(data.DataStore{sendData.Key, ""})
	}
}

func (self *Ring) callForSuccessor(myKey int, address string) *data.GroupMember {

	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	//Get Successor

	argi := &myKey
	fmt.Printf("myKey : %d", myKey)
	var response *data.GroupMember
	err = client.Call("Ring.GetSuccessor", argi, &response)
	//fmt.Println(response.Id)
	if response == nil {
		fmt.Println("No successor : only member in group")
	}
	self.Successor = response
	fmt.Println("Found Successor")
	//You might as well add the successor to the table as well
	self.updateMember(self.Successor)
	return response

}

// Gossip members from current table to a random member
func (self *Ring) doUserTableGossip() {
	if self.Active == false {
		return
	}
	tableLength := self.UserKeyTable.Len()

	// Nobody in the list yet
	if tableLength < 1 {
		return
	}
	receiver := self.getRandomMember()
	for _, subject := range self.Usertable {
		if subject.Id != receiver.Id {
			self.doGossip(subject, receiver)
		}
	}
}

func (self *Ring) getRandomMember() *data.GroupMember {

	tableLength := self.UserKeyTable.Len()

	receiverIndex := rand.Int() % tableLength

	//Arbitrary
	start := self.UserKeyTable.Min()

	var receiver *data.GroupMember
	var receiverAddrItem rbtree.Item
	receiverAddrItem = nil

	for i := 0; i < tableLength; i++ {
		if receiverIndex == i {
			receiverAddrItem = start.Item()
			break
		}
		start = start.Next()
	}
	if receiverAddrItem != nil {
		receiverAddress := receiverAddrItem.(locationStore).value
		receiver = self.Usertable[receiverAddress]
	} else {
		fmt.Println("You are doomed")
	}
	return receiver
}
func (self *Ring) doGossip(subject, receiver *data.GroupMember) (err error) {
	// The message we are sending over UDP, subject can be nil
	msg := "GOSSIP|%|" + data.Marshal(subject)
	return self.sendMessageWithPort(msg, receiver.Address)
}

func (self *Ring) sendMessageWithPort(msg, address string) (err error) {
	msg = self.Port + "<PORT>" + msg
	return sendMessage(msg, address)
}

func sendMessage(message, address string) (err error) {
	var raddr *net.UDPAddr
	if raddr, err = net.ResolveUDPAddr("udp", address); err != nil {
		log.Panic(err)
	}

	var con *net.UDPConn
	con, err = net.DialUDP("udp", nil, raddr)
	//log.Printf("Sending '%s' to %s..", message, raddr)
	logger.Log("INFO", "Sending "+message)
	if _, err = con.Write([]byte(message)); err != nil {
		log.Panic("Writing to UDP:", err)
		logger.Log("ERROR", "Writing to UDP")
	}

	return
}

func createUDPListener(hostPort string) (conn *net.UDPConn, err error) {

	var udpaddr *net.UDPAddr
	if udpaddr, err = net.ResolveUDPAddr("udp", hostPort); err != nil {
		return
	}

	conn, err = net.ListenUDP("udp", udpaddr)
	if err != nil {
		log.Fatal("listen error:", err)
	}
	fmt.Println("UDP listener created")

	return
}

func (self *Ring) createTCPListener(hostPort string) {

	var tcpaddr *net.TCPAddr
	tcpaddr, err := net.ResolveTCPAddr("tcp", hostPort)
	if err != nil {
		return
	}
	//arith := new(Arith)
	rpc.Register(self)
	rpc.HandleHTTP()

	conn, err := net.ListenTCP("tcp", tcpaddr)
	if err != nil {
		log.Fatal("listen error:", err)
	}
	go http.Serve(conn, nil)
}
