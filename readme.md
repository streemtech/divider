# Divider 
Divider is a way to set up a work division system so that a golang program can easily be split into multiple nodes, allowing for multiple processors of the same data.

Able to be implemented with multiple different backend systems, the goal of Divider is to provide a simple interface for assigning and tracking what nodes have what work. The different sub-libraries allow for creation of different implementations of Divider so that, depending on the production environment, it is possible to easily use Divider by attaching it to an already existing redis or postgres instance for example.

## How To Use Divider

### Keys and Work
Divider operates by listening for keys of work to be done. In most cases, this will work by matching a master key such as `/process/me/1234` matching `/process/me/`. Divider then splits the work to the different nodes based on the node's affinity, and that key is sent to the node to process over the channel provided via `GetReceiveStartProcessingChan()`.
If at some point divider determines the node no longer needs to process that work, the key is sent on the stop channel provided via `GetReceiveStopProcessingChan()`. 
The node should then confirm that it has completed processing via `ConfirmStopProcessing()`.
If the node determines that it can no longer process the work, or is done processing the work etc, it can manually declare that it is done via `SendStopProcessing()`.
There also exists `GetAssignedProcessingArray()` which returns an array of keys that the node is supposed to be processing. 
Its normally a good idea to compare the list to a node's list of keys being worked on for redundancy.

### Affinity
Divider determines what work is to be given to it's node based on the affinity of that node in comparison to the other nodes. 

While each implementation may treat affinity differently, there are some common rules that should be followed. 

1. Lower affinities mean *more\** work goes to that node, while maxInt should be hard coded to mean no work goes to that node.
    * *more could be proportional, or could be the smallest affinity gets all work etc. This is left up to the implementation.
2. Adjusting Affinity will not cause the node to stop processing current work. That is to be handled by the node to call `SendStopProcessing()`
3. AlterAffinity is concurrency safe
4. The method for dividing work based on Affinity should be assumed to be the same across all nodes.

Outside the rules listed above, there are 3 functions provided by Divider. 
`GetAffinity()` returns the current affinity. This should normally only be used to determine if work should be dropped or for logging/statistics.
`SetAffinity()` allows for the node to set its current affinity. A common method would be to set the affinity to the number of current keys being processed.
`AlterAffinity()` allows for the concurrency safe manipulation of the affinity. The affinity is increased by the integer passed in, meaning to decrease the affinity, simply pass a negative number. 

### Cleanup
When the node needs to stop processing, there are several different manners of doing so. 

`Flush()` will being sending the stop processing messages down the channel so that the node can finish up processing them. It also sets the affinity to maxInt so that the node no longer receives work. Flush should only return when it is aware that all nodes have completed processing.
`Cancel()` cleans up and closes all connections. It also removes the node from the list of processing nodes.
`FlushAndCancel()` is a helper to call `Flush()` and `Cancel()` one after the other.


## The Interface

```go
type Divider interface {
	Flush()
	Cancel()
	FlushAndCancel()
	GetAssignedProcessingArray() []string
	GetReceiveStartProcessingChan() *<-chan string
	GetReceiveStopProcessingChan() *<-chan string
	ConfirmStopProcessing(string)
	SendStopProcessing(string)
	SetAffinity(Affinity)
	GetAffinity() Affinity
	AlterAffinity(Affinity)
}
```


## Implementations
