require "redis"
r = Redis.new(unixsocket: "/var/run/redis/redis.sock")

class Array(T)
  def to_h2
    raise "Size of array must be even number" if (self.size & 1) == 1
    h = Hash(typeof(self[0]), typeof(self[1])).new
    return h unless self.size > 0
    (self.size >> 1).times { |i| h[self[i << 1]] = self[(i << 1) + 1] }
    h
  end
end

skeys = r.keys("s*")
svcs = Array(Redis::Future).new
r.pipelined do |rp|
  skeys.each do |skey|
    svcs << rp.hgetall(skey).as(Redis::Future)
  end
end
gh = Hash(String, Hash(Int32, Hash(String, (Array(Int32) | String | Int32)))).new
gh["s"] = Hash(Int32, Hash(String, (Array(Int32) | String | Int32))).new
svcs.each { |v|
  svc = v.value
  if svc.is_a?(Array(Redis::RedisValue))
    lh = svc.to_h2
    svcid = lh["serviceid"]
    if svcid.is_a?(String)
      gh["s"][svcid.to_i] = {"abc" => "def"}
    end
  end
}
