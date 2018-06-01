require "./SomeRedis/*"
require "redis"
require "benchmark"
module SomeRedis
  # TODO Put your code here
  r=Redis.new
  puts Benchmark.realtime {
    keys=r.keys("*")
    reslt={} of String => Redis::Future
    r.pipelined do |pipel|
          reslt=keys.each_with_object({} of String => Redis::Future) {|k,o|
                  if k.is_a?(String)
                          o[k]=pipel.hgetall(k).as(Redis::Future)
                  end
          }
    end
    cache2=reslt.each_with_object({} of String => Hash(String, Redis::RedisValue)) do |v,c|
          c[v.first]=v.last.value.as(Array(Redis::RedisValue))
          		.each_slice(2)
          		.each_with_object({} of String => Redis::RedisValue) {|sl2,hsh|
          			hsh[sl2[0].to_s]=sl2[1]
          		}
    end
  }
#  l.to_h
#  if l.is_a?(Array(Redis::RedisValue))
#	h=l.try &.to_h
#	p h
#  end
end
