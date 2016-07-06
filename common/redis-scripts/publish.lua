-- script.version = 1

local event, payload = ARGV[1], ARGV[2]
local key_name = 'hmq:event:' .. event

local targets = redis.call('smembers', key_name)

for i, queue in ipairs(targets) do
   redis.call('lpush', queue, payload)
end

return redis.status_reply('OK')
