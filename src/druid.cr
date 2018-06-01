require "json"
require "auto_msgpack"
require "redis"
require "kemal"
require "./macroDefinitions/json"
require "./macroDefinitions/msgpack"
require "./objectTypes/mixins/*"
require "./objectTypes/*"
require "./applicationClasses/Druid"


module DruidWebApp
	DFLT_SERVICE_ID = 9594
	
    redc = Redis.new
    druid = Druid.new
    
    before_all do |env|
    	env.response.content_type = "application/json"
    end
    
    get "/service/:serviceid" do |env|
    	if (svcid = env.params.url["serviceid"]) && svcid.is_a?(String) && svcid=~/^s?\d+$/
	    	druid.svc_branch_get(redc, (svcid[0] == 's' ? svcid[1..-1] : svcid).to_i).to_json
	    else
	    	halt env, status_code: 404, response: %q({"error": "Wrong service identificator"})
	    end
	rescue ex
		halt env, status_code: 503, response: {"error": "Unhandled exception #{ex.message}"}.to_json
    end
    
    Kemal.run
end
