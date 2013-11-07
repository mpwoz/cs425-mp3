cs425-mp3
=========

Chord-like distributed hashtable



Modules
-------

_gossip_: The gossiping module, used for exchanging location information

_ring_: Handles distributing keys/machines on the ring

_usertable_: Machines, addresses, locations on ring. Allows updating a machine's
          location. May be redundant if we put all this functionality in 'ring'
          instead.





