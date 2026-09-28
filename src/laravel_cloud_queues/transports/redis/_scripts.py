"""Laravel-style queue operations, executed atomically by Redis/Valkey.

All scores use Redis TIME, never a worker's wall clock. TIME followed by writes is
supported by effects replication (the only script replication mode in Redis 7+
and Valkey 8). The integration suite exercises these scripts on the real server.
"""

_TIME = """
local clock = redis.call('TIME')
local now = tonumber(clock[1]) + tonumber(clock[2]) / 1000000
"""

SEND = (
    _TIME
    + """
if tonumber(ARGV[2]) > 0 then
    redis.call('zadd', KEYS[2], now + tonumber(ARGV[2]), ARGV[1])
else
    redis.call('rpush', KEYS[1], ARGV[1])
    redis.call('rpush', KEYS[4], 1)
end
return 1
"""
)

RESERVE = (
    _TIME
    + """
-- Bound each migration batch so a backlog cannot monopolize the server.
for _, source in ipairs({KEYS[2], KEYS[3]}) do
    local due = redis.call('zrangebyscore', source, '-inf', now, 'LIMIT', 0, 100)
    for _, member in ipairs(due) do
        redis.call('rpush', KEYS[1], member)
        redis.call('zrem', source, member)
        redis.call('rpush', KEYS[4], 1)
    end
end
local member = redis.call('lindex', KEYS[1], 0)
if not member then return false end
-- Validate before removing: Lua errors do not roll back prior writes.
local ok, job = pcall(cjson.decode, member)
-- cjson uses %.14g: counters above 14 digits lose integer precision.
local malformed = not ok or type(job) ~= 'table' or type(job.id) ~= 'string'
    or type(job.body) ~= 'string' or type(job.attempts) ~= 'number'
    or job.attempts < 0 or job.attempts >= 100000000000000
    or job.attempts ~= math.floor(job.attempts)
local reserved = member
if not malformed then
    job.attempts = job.attempts + 1
    reserved = cjson.encode(job)
end
redis.call('zadd', KEYS[3], now + tonumber(ARGV[1]), reserved)
redis.call('lpop', KEYS[1])
redis.call('lpop', KEYS[4])
-- A one-element array marks a raw malformed member for terminal failure by core.
if malformed then return {reserved} end
return reserved
"""
)

# A late owner must not revive an expired lease even before another worker migrates it.
_OWNED = (
    _TIME
    + """
local expires = redis.call('zscore', KEYS[3], ARGV[1])
if not expires or tonumber(expires) <= now then return 0 end
"""
)

COMPLETE = (
    _OWNED
    + """
return redis.call('zrem', KEYS[3], ARGV[1])
"""
)

RELEASE = (
    _OWNED
    + """
redis.call('zadd', KEYS[2], now + tonumber(ARGV[2]), ARGV[1])
redis.call('zrem', KEYS[3], ARGV[1])
return 1
"""
)

RENEW = (
    _OWNED
    + """
redis.call('zadd', KEYS[3], 'XX', now + tonumber(ARGV[2]), ARGV[1])
return 1
"""
)
