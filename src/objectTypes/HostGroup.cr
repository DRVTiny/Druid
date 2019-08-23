module Cache2
	class HostGroup
		mp_struct({ groupid: Int32, name: String })
		
		def id
			@groupid
		end
	end
end
	