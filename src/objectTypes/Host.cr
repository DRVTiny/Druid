module Cache2
    class Host
	    mp_struct({
            hostid: UInt32,
            serviceid: Int32,
            status: UInt8,
            maintenance_status: UInt8,
            host: String,
            name: String
	    })
	    def id
	    	@hostid
	    end
	end
end
