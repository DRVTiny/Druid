module Cache2
    class Service
    	{% begin %}
    	{% service_struct = <<-EOSTRUCT
		{
            serviceid: Int32,
            triggerid: Int32?,
            lostfunk: Float64,
            parents: Array(Int32)?,
            dependencies: Array(Int32)?,
            name: String,
            algorithm: UInt8,
            nestd: UInt8,
            failts: Float64?,
            zloid: String?,
            maintenance_flag: UInt8?
        }
		EOSTRUCT
    	%}
    	mp_struct({{service_struct.id}})
    	
    	def id
    		@serviceid
    	end

        {% end %}
    end
end
