require "json"
require "redis"
require "msgpack"
require "benchmark"

module LoadS

    REDIS_DB_N=9

    class Service
            MessagePack.mapping({
                    failts: UInt32,
                    serviceid: UInt32,
                    lostfunk: Float64,
                    dependencies: Array(UInt32),
                    name: String,
                    algorithm: UInt8,
            })
    end

    class Host
            MessagePack.mapping({
                    hostid: UInt32,
                    status: UInt8,
                    maintenance_status: UInt8,
                    host: String,
                    name: String
            })
    end

    class Trigger
            MessagePack.mapping({
                    triggerid: UInt32,
                    svcpath: Array(UInt32),
                    priority: UInt8,
                    status: UInt8,
                    state: UInt8,
                    value: UInt8,
            })
    end
    
    puts Benchmark.realtime {
        jsonZobjs=File.open(ARGV[0], mode: "r")
        redc=Redis.new
        redc.select(REDIS_DB_N)
        redc.flushdb
        redc.pipelined do |redpip|
            zobjs=JSON.parse(jsonZobjs).each do |k,v|
            	hv = v.as_h
            	mpval_ptr = hv.to_msgpack
            	p v.raw if k == "s10637" || k == "s12237"
      		redpip.set(k.as_s, String.new(hv.to_msgpack))
            end
        end
        jsonZobjs.close
    }
#	zobjs.each
#	p zobjs
end
