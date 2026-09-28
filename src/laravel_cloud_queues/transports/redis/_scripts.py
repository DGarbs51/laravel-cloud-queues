"""The Lua scripts that perform Laravel-style queue operations atomically in Redis.

All scores use the Redis ``TIME`` command, never a worker's wall clock. Calling
``TIME`` before writes is supported by effects replication, which is the only script
replication mode in Redis 7+ and Valkey 8. The integration suite runs these scripts
against a real server.
"""

_TIME = """
local clock = redis.call('TIME')
local now = tonumber(clock[1]) + tonumber(clock[2]) / 1000000
"""
"""The script prelude that reads the current Redis time into ``now``."""

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
"""The script that pushes a job onto the queue, or onto the delayed set when delayed."""

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
"""The script that migrates due jobs and reserves the next job on the queue.

Malformed jobs are returned as a one-element array so core can fail them terminally.
"""

# A late owner must not revive an expired lease even before another worker migrates it.
_OWNED = (
    _TIME
    + """
local expires = redis.call('zscore', KEYS[3], ARGV[1])
if not expires or tonumber(expires) <= now then return 0 end
"""
)
"""The script prelude that returns 0 unless the caller still holds an unexpired reservation."""

COMPLETE = (
    _OWNED
    + """
return redis.call('zrem', KEYS[3], ARGV[1])
"""
)
"""The script that deletes a reserved job."""

RELEASE = (
    _OWNED
    + """
redis.call('zadd', KEYS[2], now + tonumber(ARGV[2]), ARGV[1])
redis.call('zrem', KEYS[3], ARGV[1])
return 1
"""
)
"""The script that moves a reserved job onto the delayed set."""

RENEW = (
    _OWNED
    + """
redis.call('zadd', KEYS[3], 'XX', now + tonumber(ARGV[2]), ARGV[1])
return 1
"""
)
"""The script that extends a job's reservation."""
