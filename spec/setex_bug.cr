require "redis"

r = Redis.new.tap &.select(4)
r.setex("s11", 2, "abc")
sleep 1
p r.get("s11")
sleep 2.5
p r.get("s11")


