macro json_fields (fields)
    {% for fldName, fldType in fields %}
    	{% if fldType.stringify.includes?("::Nil") %}
	    	@{{fldName}}.try do |v|
	    		json.field "{{fldName}}", v
	    	end
	    {% else %}
	    	json.field "{{fldName}}", @{{fldName}}
	    {% end %}
    {% end %}
end
