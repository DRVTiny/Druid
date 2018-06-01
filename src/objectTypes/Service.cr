module Cache2
    class Service
    	{% begin %}
    	{% service_struct = <<-EOSTRUCT
		{
            serviceid: Int32,
            lostfunk: Float64,
            dependencies: Array(Int32)?,
            name: String,
            algorithm: UInt8,
            nestd: UInt8,
        }
		EOSTRUCT
    	%}
    	mp_struct({{service_struct.id}})
    	
    	def id
    		@serviceid
    	end
    	
        def to_json (json)
            json.object do
            	json_class_fields({{service_struct.id}})
                if (deps = self.dependencies) && deps.is_a?(Array(Int32)) && (deps.size>0)
                    json.field "dependencies" do
                        json.array do
                            deps.each do |child_serviceid|
                                json.number child_serviceid
                            end
                        end
                    end
                end
            end
        end
        {% end %}
    end
end
