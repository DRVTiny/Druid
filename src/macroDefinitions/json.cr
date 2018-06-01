macro json_class_fields (fields)
    {% for fldName, fldType in fields %}
    	{% unless fldType.stringify.starts_with?("Array(") %}
            {% if fldType.stringify.includes?("::Nil") %}
                if (fld=self.{{fldName}}) && !fld.is_a?(Nil)
                   json.field "{{fldName}}", self.{{fldName}}
                end
            {% else %}
                json.field "{{fldName}}", self.{{fldName}}
            {% end %}
	    {% end %}
    {% end %}
end
