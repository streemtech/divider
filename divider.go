package divider

import (
	"context"
	"iter"
)

// Divider provides a very simple method of getting and sending start and stop orders
type Divider interface {
	//Start is the trigger to make the divider begin checking for keys, and returning those keys to the channels.
	//No values should return to the channels without start being called.
	//Start and stop processing can be called without calling start.
	StartWorker(context.Context)

	//Stop begins the process of stopping processing of all assigned keys.
	//Releasing these keys via stop allows them to immediately be picked up by other nodes.
	//Start must be called to begin picking up work keys again.
	StopWorker(context.Context)

	//StopProcessing takes in a string of a key that this node is no longer processing.
	//This is to be used to disable the work temporairly. Should be used when the work fetcher will not return the work anymore.
	StopProcessing(context.Context, ...string) error

	//StartProcessing adds this key to the list of work to be completed. This will temporairly force a worker to start working on the work.
	//Should be used when the work fetcher will start returning the work from now on.
	StartProcessing(context.Context, ...string) error

	//returns all work assigned to this particular divider node.
	//Other methods may produce generated methods of assigning/removing work, but this is the basic one.
	GetWork(ctx context.Context) (iter.Seq[string], error)
}
