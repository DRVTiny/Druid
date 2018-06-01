module Cache2
	class HostGroup
		mp_struct({ groupid: Int32, name: String }, 1)
		def id
			@groupid
		end
	end
end
	