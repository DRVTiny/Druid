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
        {% end %}
    end
end
