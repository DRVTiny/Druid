require "json"
require "auto_msgpack"
require "redis"
require "benchmark"
require "./macroDefinitions/json"
require "./macroDefinitions/msgpack"
require "./objectTypes/mixins/*"
require "./objectTypes/*"
{% begin %}
{% flUseFibers = env("USE_FIBERS") != nil %}
{% className = flUseFibers ? "DruidF" : "Druid" %}
require "./applicationClasses/{{className.id}}"

module MyTestApp
	DFLT_SERVICE_ID = 9594
	
    puts Benchmark.measure {
        serviceid = ARGV[0]? ? ARGV[0] : DFLT_SERVICE_ID
        {% if flUseFibers %}
        {% puts "Fibers will be used".id %}
        {% else %}
        r = Redis.new
        {% end %}
        druid = {{className.id}}.new
        h = druid.svc_branch_get( {% unless flUseFibers %} r, {% end %} serviceid.to_i )
        puts "received #{h.keys.size} keys"
    }
end
{% end %}
