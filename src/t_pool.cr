require "redis"

r=Redis.new
r.select(8)
puts "Generic:"
p r.mget(9594,12237)

rp=Redis::PooledClient.new
rp.select(8)
puts "Pooled:"
p rp.mget(9594,12237)
