module Cache2
    class Trigger
    	{% begin %}
    	{% trigger_struct = <<-EOSTRUCT
    	{
            triggerid: UInt32,
            svcpath:   Array(Array(Int32)),
            priority:  UInt8,
            status:	   UInt8,
            state: 	   UInt8,
            value:     UInt8
        }
		EOSTRUCT
		%}
        mp_struct({{trigger_struct.id}})
        def id
        	@triggerid
        end
        def to_json (json : JSON::Builder)
            json.object do
            	json_class_fields({{trigger_struct.id}})
                json.field "svcpath" do
                    json.array do
                        self.svcpath.each do |dep|
                        	json.array do
                                dep.each do |subdep|
                                	json.number subdep
                                end
                            end
                        end
                    end
                end            	
            end
        end
        {% end %}
    end
end
