-- main.lua is responsible for the cleanup and running of this implementaiton of the divider
-- KEY1 is the poll meta key. (poll:__meta)
-- KEY2 is the poll master key (poll)
-- ARG1 UUID of this node
-- ARG2 Current Time (Unix Seconds)
-- ARG3 timeout time (N Seconds)
-- ARG4 affinity


--need some way to store timeouts.
--need some way to check what work is assigned to what node
--

--**********************************
local metaKey = KEYS[1]
local masterKey = KEYS[2]
local nodeUUID = ARGV[1]
local currentTime = ARGV[2]
local timeout = ARGV[3]
local deadTime = currentTime-timeout
local nodeAffinity = ARGV[4]


local nodeSetKey = metaKey .. ":nodes"
local affinityKey = metaKey .. ":affinity"
local affinitySumKey = metaKey .. ":affinity:sum"
local lastAffinitySumKey = metaKey .. ":affinity:sum:last" -- the store for the last affinity when checking to load new values. Compare to affinty to allow for only updating affinity if I need to. 
local chanceKey = metaKey .. ":assignmentChance"
local workKey = metaKey.."work" --work stores where the work is assigned. work:id stores what work was assigned to that key.






--set the work to having that node for it;s worker, and add the work to the set of work that this node is working on.
function assignWorkToNode(workId, NodeId) 
	redis.log(3, "asigning work To Node: " .. workId .." Node: ".. NodeId)
	redis.call("hadd", workKey, workId, NodeId) --set what node is working on this work
	redis.call("sadd", workKey..":"..NodeId) --set that this node has this work. 
end

function assignWork (workId)
	redis.log(3, "Asigning Work: ".. workId)
	local node = redis.call("", chanceKey, math.rand(),2,  "LIMIT", 0 ,1)
	assignWorkToNode(workId, node[1])
end

function deleteWork (workId)
	redis.log(3, "deleting work: ".. workId)
	local worker = redis.call("hget", workKey, workId) --get what node is working on this work
	redis.call("hdel", workKey, workId)
	redis.call("srem", workKey..":"..worker)

end

--update the list of affinities's chances in the key. wont update if not needed.
function updateAffinityChances()
	redis.log(3, "updating Affinity.")

	local sum = redis.call("get",affinitySumKey)
	local lastSum = redis.call("get", lastAffinitySumKey)
	if sum == lastSum then
		return
	end

	local affinities = redis.call("hgetall", affinityKey)

	local score = 0
	local key = ""
	local weight = 0
	for i,val in ipairs(affinities) do

		if (i % 2 == 1) then
			key = val
		else
			weight = val
			redis.call("zset", chanceKey, key, score/weight)
		end
	end

	redis.call("zset", chanceKey, key, 1)
	redis.call("set",lastAffinitySumKey, sum)

end




--update the affinity for a specific node, and update the sum affinity.
function updateAffinity (id, affinity)
	redis.log(3, "updating affinity " .. id .." to ".. affinity)
	local exists = redis.call("hexists", affinityKey, id)

	if exists == 1 then
		local current = redis.call("hget", affinityKey, id)
		redis.call("hset", affinityKey, id, affinity)
		redis.call("INCRBY", affinitySumKey, affinity-current)
	else
		redis.call("hset", affinityKey, id, affinity)
		redis.call("INCRBY", affinitySumKey, affinity)
	end
end

--remove a node from the different lists, removing its affinity. also remove work not assigned from the list.
function deleteNode (id)
	redis.log(3, "deleting node " .. id )
	updateAffinity(id, 0)
	redis.call("zrem", nodeSetKey, id )
	redis.call("srem", affinityKey, id)
	local nodeWorkKey = workKey..":"..id
	--go through the work that this node was assigned and remove it.
	local assignedWork = redis.call("SMEMBERS", nodeWorkKey)
	for _,wokr in ipairs(assignedWork) do
		redis.call("hdel", workKey, work)
	end
	redis.call("del", nodeWorkKey)
end

--update values to make sure that this node is recorded.
function update()
	redis.log(3,"update")
	--Update set of live nodes to contain self, with current time as the score.
	redis.call('zadd', nodeSetKey, nodeUUID, currentTime)
	--update the affinity.
	updateAffinity(nodeUUID, nodeAffinity)
end

--clean up all old nodes, if they have been dropped.
function cleanup()
	redis.log(3,"cleanup")
	deadNodes = redis.call('ZRANGEBYSCORE', nodeSetKey, 0, deadTime)
	--Set the key/value lists for the work asignments.
	for _,deadNode in ipairs(deadNodes) do
		deleteNode(id)
	end
end


--assign work to nodes.
function assign()
	redis.log(3, "asigning.")
	updateAffinityChances()
	--get the current work
	local work = redis.call("pubsub", "channels", masterKey)
	--get the previous work
	local oldWork redis.call("sget", currentWorkKey)
	--save the current work to currentWorkKey
	redis.call("del", currentWorkKey)
	redis.call("sadd", currentWorkKey, unpack(work))
	--savePreviousWork to previousWorkKey
	redis.call("del", oldWorkKey)
	redis.call("sadd", oldWorkKey, unpack(oldWork))
	--get the difference between the two works
	local toDelete = redis.call("sdiff", oldWorkKey, currentWorkKey)
	local toAdd= redis.call("sdiff", currentWorkKey, oldWorkKey)
	
	for _,work in ipairs(toDelete) do
		deleteWork(work)
	end
	for _,work in ipairs(toAdd) do
		assignWork(work)
	end

end



update()
cleanup()
assign()