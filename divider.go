package divider

import (
	"fmt"
)

//Informer is a simple way to pass messages from the Divider to the external system.
//TODO need to add a debug here likely.
type Informer interface {
	Infof(message string, args ...interface{})
	Errorf(message string, args ...interface{})
	Debugf(message string, args ...interface{})
}

//Divider provides a very simple method of getting and sending start and stop orders
type Divider interface {
	//Start is the trigger to make the divider begin checking for keys, and returning those keys to the channels.
	//No values should return to the channels without start being called.
	Start()

	//Stop begins the process of stopping processing of all assigned keys.
	//Releasing these keys via stop allows them to immediately be picked up by other nodes.
	//Start must be called to begin picking up work keys again.
	Stop()

	//Close shuts down, closes and cleans up the process.
	//If called before flushed, processing keys will be timed out instead of released.
	Close()

	//StopAndClose is a simple helper to call Stop and Close.
	StopAndClose()

	//GetAssignedProcessingArray returns a string array that represents the keys that this node is set to process.
	GetAssignedProcessingArray() []string

	//GetReceiveStartProcessingChan returns a channel of strings.
	//The string from this channel represents a key to a processable entity.
	//This particular channel is for receiving keys that this node is to begin processing.
	GetReceiveStartProcessingChan() <-chan string

	//GetReceiveStopProcessingChan returns a channel of strings.
	//The string from this channel represents a key to a processable entity.
	//This particular channel is for receiving keys that this node is to stop processing.
	GetReceiveStopProcessingChan() <-chan string

	//ConfirmStopProcessing takes in a string of a key that this node is no longer processing.
	//This is to be used to confirm that the processing has stopped for a key gotten from the GetReceiveStopProcessingChan channel.
	//To manually release processing of a key, use SendStopProcessing instead.
	//ConfirmStopProcessing is expected to be required for the proper implementation of Flush()
	ConfirmStopProcessing(string)

	//SendStopProcessing takes in a string of a key that this node is no longer processing.
	//This is to be used to release the processing to another node.
	//To confirm that the processing stoppage is completed, use ConfirmStopProcessing instead.
	SendStopProcessing(string)

	//SetAffinity allows for the affinity to be set by the node so that some controll can be implemented over what nodes receive work.
	SetAffinity(Affinity)
	//GetAffinity returns the current affinity. Most cases that use get and then Set affinity would be better off using AlterAffinity.
	GetAffinity() Affinity

	//AlterAffinity takes in an integer and increases the current affinity by that amount.
	//This method of updating the affinity should be implemented as concurrency safe.
	AlterAffinity(Affinity)
}

//Affinity is an int64 that allows for the nodes to distribute work
//Each library is responsible for their own implementation of the affinity, however, some rules should be followed.
//1: Lower affinities mean more work goes to that node.
//       (more could be proportional, or could be the smallest affinity gets all work etc. This is left up to the implementation.)
//2: Adjusting Affinity will not cause the node to stop processing current work. That is to be handled by the node to call SendStopProcessing()
//3: AlterAffinity is concurrency safe
//4: The method for dividing work based on Affinity should be assumed to be the same across all nodes.
type Affinity int64

//DefaultLogger is a quick and dirty logger that matches the Informer interface.
type DefaultLogger struct{}

//Infof for the DefaultLogger prints the message to the deafult output with the prefex "INFO  :
func (d DefaultLogger) Infof(message string, args ...interface{}) {
	fmt.Printf("INFO : %s\n", fmt.Sprintf(message, args...))
}

//Errorf for the DefaultLogger prints the message to the deafult output with the prefex "ERROR:"
func (d DefaultLogger) Errorf(message string, args ...interface{}) {
	fmt.Printf("ERROR: %s\n", fmt.Sprintf(message, args...))
}

//Debugf for the DefaultLogger prints the message to the deafult output with the prefex "DEBUG:"
func (d DefaultLogger) Debugf(message string, args ...interface{}) {
	fmt.Printf("DEBUG: %s\n", fmt.Sprintf(message, args...))
}

//EmptyLogger is a quick and dirty logger that prints nothing, which matches the Informer interface.
type EmptyLogger struct{}

//Infof for the EmptyLogger prints nothing
func (d EmptyLogger) Infof(message string, args ...interface{}) {
}

//Errorf for the EmptyLogger prints nothing
func (d EmptyLogger) Errorf(message string, args ...interface{}) {
}
