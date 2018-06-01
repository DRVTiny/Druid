require "json"
require "redis"
require "msgpack"
require "benchmark"
macro json_class_fields (fields)
    {% for field in fields %}
           json.field "{{field}}", self.{{field}}
    {% end %}
end

macro mp_class (class_name, props)
    class {{class_name}}
        MessagePack.mapping({{props}})
        def to_json(json : JSON::Builder)
            json.object do
            	json_class_fields({{props}})
            end
        end
    end
end

module LoadS

    REDIS_DB_N=8

    
    class Service
            MessagePack.mapping({
#                    failts: UInt32,
                    serviceid: Int32,
                    lostfunk: Float64,
                    dependencies: Array(Int32),
                    name: String,
                    algorithm: UInt8,
                    nestd: UInt8,
            })

            def to_json (json)
            	json.object do
            	    json_class_fields [name,serviceid,algorithm,lostfunk,nestd]
                    json.field "dependencies" do
                        json.array do
                            self.dependencies.each do |dep|
                                json.number dep
                            end
                        end
                    end
            	end
            end
    end
    
    mp_class(Assoc, { zloid: String })
    mp_class(Host, {
            hostid: UInt32,
            status: UInt8,
            maintenance_status: UInt8,
            host: String,
            name: String
    })
    mp_class(HostGroup, { name: String, groupid: UInt32 })
    
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
    
    serviceid=ARGV[0]? ? ARGV[0] : "9594"
    redc=Redis.new
    redc.select(REDIS_DB_N)
    assocs={ h: {} of Int32 => Bool, g: {} of Int32 => Bool, t: {} of Int32 => Bool }
    if (svc_s=redc.get(serviceid)).is_a?(String)
    	svc=Service.from_msgpack(svc_s[4..-1].to_slice)
    	begin
    		assoc=Assoc.from_msgpack(svc_s[4..-1].to_slice)
    	rescue
    		puts "This service contains no zloid attribute"
    	else
    		puts "zloid is #{assoc.zloid}"
    		assocs[assoc.zloid[0].to_s][assoc.zloid[1..-1].to_i]=true
    	end
		puts svc.class
		h={"s#{serviceid}" => svc}
    	puts h.to_json
    	
    end
    p assocs.keys
#	zobjs.each
#	p zobjs
end
