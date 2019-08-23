class Druid
    DFLT_ZOTYPE_DBN = {t: 5, h: 6, g: 7, s: 8}
    ZOTYPE2LTR = {host: "h", hostgroup: "g", trigger: "t", service: "s", group: "g"}
    ZOLTR2TYPE = {t: Cache2::Trigger, h: Cache2::Host, g: Cache2::HostGroup, s: Cache2::Service}
    
    def initialize (@zotypes = DFLT_ZOTYPE_DBN)
    end
    
    def svc_branch_get ( redc : Redis, serviceid : Int32 ) : Hash(String, (Cache2::Service | Cache2::Host | Cache2::HostGroup | Cache2::Trigger))
        fill_this = {} of String => (Cache2::Service | Cache2::Host | Cache2::HostGroup | Cache2::Trigger)
        assocs = { h: {} of Int32 => Bool, g: {} of Int32 => Bool, t: {} of Int32 => Bool }
        # Select Redis Database Number which is used for Service objects storage 
        redc.select(@zotypes[:s])
        # Empty "rcrs_get_deps" closure prototype to avoid  "read before assignment" compile-time exception while trying to do closure recurse call
        rcrs_get_deps = ->(x : Array(Int32)) { true }
        rcrs_get_deps = ->(serviceids : Array(Int32)) do
            nxt_lvl_sids = redc.mget(serviceids).each_with_object([] of Int32) do |svc_s, sids|
                if svc_s.is_a?(String) && svc_s.size > 4
                    slcSerData = svc_s[4..-1].to_slice
                    begin
                        svc_o = Cache2::Service.from_msgpack( slcSerData )
                    rescue ex
                        puts "Exception #{ex.message} while processing MP: #{svc_s}"
                        exit 1
                    end
                    fill_this["s" + svc_o.serviceid.to_s] = svc_o
                    if (deps = svc_o.dependencies) && deps.is_a?(Array(Int32))
                        sids.concat(deps)
                    end
                    begin
                        assoc=Cache2::Assoc.from_msgpack( slcSerData )
                    rescue
                    # no zloid defined
                    else
                        zoltr = assoc.zloid[0].to_s
                        if assocs[zoltr]?
                            assocs[zoltr][assoc.zloid[1..-1].to_i] = true
                        end
                    end
                end
            end # <- redc.mget
            if nxt_lvl_sids.size > 0
                rcrs_get_deps.call(nxt_lvl_sids) || return false
            end
            return true               
        end # <- recursive get services
        
        rcrs_get_deps.call([serviceid])
        
        assocs.each do |zoltr, zoids_h|
            next unless (zoids = zoids_h.keys) && zoids.size>0
            redc.select(@zotypes[zoltr])
            zoids = zoids_h.keys
            redc.mget(zoids).each_with_index do |zobj_s, ki|
                if zobj_s.is_a?(String)
                    fill_this[zoltr.to_s + zoids[ki].to_s] = decode_msgpack(ZOLTR2TYPE[zoltr], zobj_s[4..-1].to_slice)
                end
            end
        end
        fill_this
    end # <- self.svc_branch_get
    
    private def decode_msgpack (klass, what2decode : Slice(UInt8))
        klass.from_msgpack(what2decode)
    end
end # <- class Druid
