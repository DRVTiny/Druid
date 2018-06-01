require "json"
require "auto_msgpack"
require "redis"
require "benchmark"
require "./macroDefinitions/json"
require "./macroDefinitions/msgpack"
require "./objectTypes/mixins/*"
require "./objectTypes/*"
require "./applicationClasses/Druid"

module MyTestApp
	DFLT_SERVICE_ID = 9594
	
    puts Benchmark.measure {
        serviceid = ARGV[0]? ? ARGV[0] : DFLT_SERVICE_ID
        redc = Redis.new    
        druid = Druid.new
        h = druid.svc_branch_get(redc, serviceid.to_i) #.to_json
        puts "received #{h.keys.size} keys"
    }
end
