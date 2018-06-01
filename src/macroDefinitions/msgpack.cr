macro mp_struct (fields, fl_gen_json=nil)
	include AutoMsgpack
	{% for fldName, fldType in fields %}
		field :{{fldName}}, {{fldType}}
	{% end %}
	{% if fl_gen_json %}
		def to_json(json : JSON::Builder)
			json.object do
				json_class_fields({{fields}})
			end
		end
	{% end %}
end
