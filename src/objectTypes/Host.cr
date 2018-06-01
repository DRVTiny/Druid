module Cache2
    class Host
	    mp_struct({
            hostid: UInt32,
            status: UInt8,
            maintenance_status: UInt8,
            host: String,
            name: String
	    }, 1)
	    def id
	    	@hostid
	    end
	end
end
