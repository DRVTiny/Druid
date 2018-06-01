require "./SomeRedis/*"
require "redis"
require "benchmark"
module SomeRedis
  # TODO Put your code here
  puts "Hello!"
  r=Redis.new
  puts Benchmark.realtime {
      cache2=r.keys("*").each_with_object({} of String => Hash(String, Redis::RedisValue)) {|zloid, rhsh|
            rhsh[zloid.to_s]=r.hgetall(zloid.to_s).each_slice(2).each_with_object({} of String => Redis::RedisValue) {|sl2,hsh| hsh[sl2[0].to_s]=sl2[1] }
      }
  }
#  p cache2
#  l.to_h
#  if l.is_a?(Array(Redis::RedisValue))
#	h=l.try &.to_h
#	p h
#  end
end
