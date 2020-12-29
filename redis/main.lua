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
local affinityKey = metaKey .. ":affinity:affinity"
local chanceKey = metaKey .. ":assignmentChance"
local workKey = metaKey..":work:assigned" --work stores where the work is assigned. work:id stores what work was assigned to that key.
local currentWorkKey = metaKey..":work:current"
local oldWorkKey = metaKey..":work:old"

local function log() 
	--redis.log(3, "\nmetaKey:" .. metaKey ..
	-- "\nmasterKey: " .. masterKey ..
	-- "\nnodeUUID: " .. nodeUUID ..
	-- "\ncurrentTime: " .. currentTime ..
	-- "\ntimeout: " .. timeout ..
	-- "\ndeadTime: " .. deadTime ..
	-- "\nnodeAffinity: " .. nodeAffinity ..
	-- "\nnodeSetKey: " .. nodeSetKey ..
	-- "\naffinityKey: " .. affinityKey ..
	-- "\nchanceKey: " .. chanceKey ..
	-- "\nworkKey: " .. workKey )

end




--set the work to having that node for it;s worker, and add the work to the set of work that this node is working on.
local function assignWorkToNode(workId, NodeId) 
	--redis.log(3, "asigning work To Node: " .. workId .." Node: ".. NodeId)
	redis.call("hset", workKey, workId, NodeId) --set what node is working on this work
	redis.call("sadd", workKey..":"..NodeId, workId) --set that this node has this work. 
end

local function assignWork (workId)
	--redis.log(3, "Asigning Work: ".. workId)
	local node = redis.call("ZRANGEBYSCORE", chanceKey, math.random(),2,  "LIMIT", 0 ,1)
	assignWorkToNode(workId, node[1])
end

local function deleteWork (workId)
	--redis.log(3, "deleting work: ".. workId)
	local worker = redis.call("hget", workKey, workId) --get what node is working on this work
	redis.call("hdel", workKey, workId)
	redis.call("srem", workKey..":"..worker)

end

--update the list of affinities's chances in the key. wont update if not needed.
local function updateAffinityChances()
	--redis.log(3, "updating Affinity.")


	local sum = 0;
	local affinityVals = redis.call("hvals", affinityKey)
	for _,val in ipairs(affinityVals) do

		sum = sum  + val
	end


	redis.call("del", chanceKey)
	local affinities = redis.call("hgetall", affinityKey)

	local score = 0
	local key = ""
	local weight = 0
	for i,val in ipairs(affinities) do

		if (i % 2 == 1) then
			key = val
		else
			weight = weight + val/sum
			redis.call("zadd", chanceKey, weight, key)
		end
	end

	redis.call("zadd", chanceKey, 1, key)

end




--update the affinity for a specific node, and update the sum affinity.
local function updateAffinity (id, affinity)
	--redis.log(3, "updating affinity " .. id .." to ".. affinity)
	local exists = redis.call("hexists", affinityKey, id)

	if exists == 1 then
		local current = redis.call("hget", affinityKey, id)
		redis.call("hset", affinityKey, id, affinity)
	else
		redis.call("hset", affinityKey, id, affinity)
	end
end

--remove a node from the different lists, removing its affinity. also remove work not assigned from the list.
local function deleteNode (id)
	--redis.log(3, "deleting node " .. id )
	redis.call("zrem", nodeSetKey, id )
	redis.call("hdel", affinityKey, id)

	local nodeWorkKey = workKey..":"..id
	--go through the work that this node was assigned and remove it.
	local assignedWork = redis.call("SMEMBERS", nodeWorkKey)
	for _,work in ipairs(assignedWork) do
		redis.call("hdel", workKey, work)
	end
	redis.call("del", nodeWorkKey)
end

--update values to make sure that this node is recorded.
local function update()
	--redis.log(3,"update")
	--Update set of live nodes to contain self, with current time as the score.
	redis.call('zadd', nodeSetKey,  currentTime, nodeUUID)
	--update the affinity.
	updateAffinity(nodeUUID, nodeAffinity)
end

local function cleanAssigned() 
	redis.log(3, "clean assignd values to make sure that no value is stuck.")
end

--clean up all old nodes, if they have been dropped.
local function cleanup()
	--redis.log(3,"cleanup")
	local deadNodes = redis.call('ZRANGEBYSCORE', nodeSetKey, 0, deadTime)
	--Set the key/value lists for the work asignments.
	for _,deadNode in ipairs(deadNodes) do
		deleteNode(deadNode)
	end

	if math.random()>0.99 then
		cleanAssigned()
	end
end

--TODO I need to do a cleanup every so often, otherwise its possible for work to get stuck
--WORK stuck means that the node no longer exists, but the work is still a value in __meat:work:assigned

--assign work to nodes.
local function assign()
	--redis.log(3, "asigning.")
	
	--get the current work
	local work = redis.call("pubsub", "channels", masterKey..":*")
	--get the previous work
	local oldWork = redis.call("hkeys", workKey)

	--save the current work to currentWorkKey
	redis.call("del", currentWorkKey)
	if #work >0 then
		redis.call("sadd", currentWorkKey, unpack(work))
	else
		--redis.log(3, "no work to assign")
	end
	--savePreviousWork to previousWorkKey
	redis.call("del", oldWorkKey)
	if #oldWork > 0 then
		redis.call("sadd", oldWorkKey, unpack(oldWork))
	else
		--redis.log(3, "no old work to assign")
	end
	--get the difference between the two works
	local toDelete = redis.call("sdiff", oldWorkKey, currentWorkKey)
	local toAdd = redis.call("sdiff", currentWorkKey, oldWorkKey)
	
	for _,work in ipairs(toDelete) do
		deleteWork(work)
	end

	if #toAdd >0 then 
		updateAffinityChances()
		for _,work in ipairs(toAdd) do
			assignWork(work)
		end
	end
end


log()

update()
cleanup()
assign()

return redis.call("smembers", workKey..":"..nodeUUID)