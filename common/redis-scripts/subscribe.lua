-- script.version = 1

local event, queue = ARGV[1], ARGV[2]
local key_name = 'hmq:event:' .. event


return redis.call('sadd', key_name, queue)
