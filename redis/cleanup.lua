--KEY1 timeoutKey (where Arg1's timout is to be reset.)
--KEY2 affinity  (where all affinities are stored)
--KEY3 workKey (where all the work is assigned.)
--ARG1 uuid
--ARG2 timeout seconds

--update the timeout on this ticker to be the most up to date.
redis.call('setex', KEYS[1] .. ARGV[1], tonumber(ARGV[2]), ARGV[1])

--check for keys without timeout. 
local nodes = redis.call('hkeys', KEYS[2]) --get everything in affinity.
local work = redis.call("hgetall",KEYS[3]) --get the work assignments.

local workKeys = {}
local workerNodes = {}

--Set the key/value lists for the work asignments.
for i,vgetVal in ipairs(work) do
	if (tonumber(i) % 2 == 1) then
		table.insert(workKeys, vgetVal)
	else
		table.insert(workerNodes, vgetVal)
	end
end

for key,missingNode in pairs(nodes) do
	local nodeKey = KEYS[1] .. missingNode
	local isThere = redis.call('exists', nodeKey)
	--redis.log(3, "value " .. nodeKey .. " IS " .. isThere)

	--if the timeout key is NO there (IE it is no longer updating)
	if isThere == 0 then 


		--delete from the work list.
		redis.call('hdel', KEYS[2], missingNode) 

		--for the list of work noddes (the node it was assigned to)
		for workerIDX,workerNode in ipairs(workerNodes) do
			--redis.log(3, "checking " .. workerNode .. " against " .. missingNode)
			if (workerNode == missingNode) then
				redis.log(3, "same value.")
				redis.call("hdel", KEYS[3], workKeys[workerIDX])
			end
		end

	end
end