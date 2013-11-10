package ring

import (
  "fmt"
  "../data"
)

/*
    The following are the calls exposed over RPC and may be called from a remote machine
    They correspont to the Insert/Update/Remove/Lookup calls on the client
*/

/* Insert */
func (self *Ring) SendData(sentData *data.DataStore, success *int) error {
	inserted := self.KeyValTable.Insert(data.DataStore{(*sentData).Key, (*sentData).Value})
  *success = Btoi(inserted)
	return nil
}

/* Remove */
func (self *Ring) RemoveData(key *int, success *int) error {
	deleted := self.KeyValTable.DeleteWithKey(data.DataStore{*key, ""})
  *success = Btoi(deleted)
	return nil
}

/* Lookup */
func (self *Ring) GetData(key *int, responseData *data.DataStore) error {
	found := self.KeyValTable.Get(data.DataStore{*key, ""})
	if found == nil {
		fmt.Println("Data not found")
    *responseData = data.NilDataStore()
	} else {
		*responseData = found.(data.DataStore)
	}
	return nil
}

/* Update */
func (self *Ring) UpdateData(sentData *data.DataStore, success *int) error {
	// Delete the current data, then add the new
	self.KeyValTable.DeleteWithKey(data.DataStore{(*sentData).Key, ""})
	inserted := self.KeyValTable.Insert(data.DataStore{(*sentData).Key, (*sentData).Value})
  *success = Btoi(inserted)
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
func Btoi(b bool) (int) {
  if b {
    return 1
  }
  return 0
}

