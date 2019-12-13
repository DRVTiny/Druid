macro mp_struct(fields, fl_gen_json = true)
	include MessagePack::Serializable
	
	{% for fldName, fldType in fields %}
		property {{fldName}} : {{fldType}}
	{% end %}
	
	{% if fl_gen_json %}
		def to_json(json : JSON::Builder)
			json.object do
				json_fields({{fields}})
			end
		end
		
		def to_json(io : IO)
			JSON.build(io) do |json|
				to_json(json)
			end
		end
	{% end %}
end
