class DruidF
  ENCODE_PFX                     = "{MP}"
  ENCODE_PFX_LENGTH              = ENCODE_PFX.size
  N_FIBERS_P2                    = 3
  N_FIBERS                       = 1 << N_FIBERS_P2
  N_MIN_SVC_OBJS_TO_USE_FIBERS   = N_FIBERS << 5
  N_MIN_ASSOC_OBJS_TO_USE_FIBERS = N_FIBERS * 50
  DFLT_ZOTYPE_DBN                = {t: 5, h: 6, g: 7, s: 8}
  ZOTYPE2LTR                     = {host: "h", hostgroup: "g", trigger: "t", service: "s", group: "g"}
  ZOLTR2TYPE                     = {t: Cache2::Trigger, h: Cache2::Host, g: Cache2::HostGroup, s: Cache2::Service}
  ZOLTR2ID_ATTR                  = {t: "triggerid", h: "hostid", g: "groupid", s: "serviceid"}

  @redc : Array(Redis)

  def initialize(@zotypes = DFLT_ZOTYPE_DBN)
    @redc = (1..N_FIBERS).map { Redis.new }
  end

  def svc_branch_get(serviceid : Int32) : Hash(String, (Cache2::Service | Cache2::Host | Cache2::HostGroup | Cache2::Trigger))
    fill_this = {} of String => (Cache2::Service | Cache2::Host | Cache2::HostGroup | Cache2::Trigger)
    assocs = {h: {} of Int32 => Bool, g: {} of Int32 => Bool, t: {} of Int32 => Bool}

    # Select Redis Database Number which is used for Service objects storage
    @redc.each { |redc| redc.select(@zotypes[:s]) }
    # Empty "rcrs_get_deps" closure prototype to avoid  "read before assignment" compile-time exception while trying to do closure recurse call
    rcrs_get_deps = ->(x : Array(Int32)) { true }
    rcrs_get_deps = ->(serviceids : Array(Int32)) do
      nxt_lvl_sids = if serviceids.size >= N_MIN_SVC_OBJS_TO_USE_FIBERS
                       svcs = [] of String
                       n_objs_per_part = serviceids.size >> N_FIBERS_P2
                       ch_wait_get_svcs = Channel(Nil).new(N_FIBERS)

                       end_index = serviceids.size - 1
                       #	            start_end = [] of Range(Int32, Int32)
                       #                (N_FIBERS - 1).downto(0) do |i|
                       #                    start_end << ((i > 0 ? (end_index - n_objs_per_part + 1) : 0).. end_index)
                       #                    end_index -= n_objs_per_part
                       #                end

                       N_FIBERS.times do |i|
                         spawn do
                           ei = end_index - i * n_objs_per_part
                           @redc[i].mget(serviceids[(i == (N_FIBERS - 1) ? 0 : ei - n_objs_per_part + 1)..ei]).each do |s|
                             svcs << s if s.is_a?(String) && s.size > 4
                           end
                           ch_wait_get_svcs.send(nil)
                         end
                       end
                       N_FIBERS.times { ch_wait_get_svcs.receive }
                       svcs
                     else
                       @redc[0].mget(serviceids)
                     end.each_with_object([] of Int32) do |svc_s, sids|
                       next unless svc_s.is_a?(String) && svc_s.size > ENCODE_PFX_LENGTH
                       svc_slice = svc_s.to_slice + ENCODE_PFX_LENGTH
                       begin
                         svc_o = Cache2::Service.from_msgpack(svc_slice)
                       rescue ex
                         puts "Exception #{ex.message} while processing MP: #{svc_s}"
                         exit 1
                       end
                       fill_this["s" + svc_o.serviceid.to_s] = svc_o
                       if (deps = svc_o.dependencies) && deps.is_a?(Array(Int32))
                         sids.concat(deps)
                       end
                       begin
                         assoc = Cache2::Assoc.from_msgpack(svc_slice)
                       rescue
                         # normal situation: no association/"zloid attribute" was defined for this service
                       else
                         zoltr = assoc.zloid[0].to_s
                         if assocs[zoltr]?
                           assocs[zoltr][assoc.zloid[1..-1].to_i] = true
                         end
                       end
                     end
      if nxt_lvl_sids.size > 0
        rcrs_get_deps.call(nxt_lvl_sids) || return false
      end
      return true
    end # <- recursive get services

    rcrs_get_deps.call([serviceid])

    ch_wait_get_assocs = Channel(Nil).new(N_FIBERS)
    assocs.each do |zoltr, zoids_h|
      next unless (zoids = zoids_h.keys) && zoids.size > 0
      zoltr_s = zoltr.to_s
      if zoids.size >= N_MIN_ASSOC_OBJS_TO_USE_FIBERS
        ass_zo = [] of String
        n_objs_per_part = zoids.size >> N_FIBERS_P2
        end_index = zoids.size - 1

        #            	start_end = [] of Range(Int32, Int32)
        end_index = zoids.size - 1
        #            	(N_FIBERS - 1).downto(0) do |i|
        #            		start_end << ((i > 0 ? (end_index - n_objs_per_part + 1) : 0).. end_index)
        #            		end_index -= n_objs_per_part
        #            	end

        N_FIBERS.times do |i|
          spawn do
            ei = end_index - i * n_objs_per_part
            redc = @redc[i]
            redc.select(@zotypes[zoltr])
            redc.mget(zoids[(i == (N_FIBERS - 1) ? 0 : ei - n_objs_per_part + 1)..ei]).each do |s|
              ass_zo << s if s.is_a?(String) && s.size > 4
            end
            ch_wait_get_assocs.send(nil)
          end
        end

        N_FIBERS.times { ch_wait_get_assocs.receive }
      else
        @redc[0].select(@zotypes[zoltr])
        ass_zo = @redc[0].mget(zoids)
      end
      ass_zo.each_with_index do |zobj_s, ki|
        next unless zobj_s.is_a?(String)
        zobj = decode_msgpack(ZOLTR2TYPE[zoltr], (zobj_s.to_slice + ENCODE_PFX_LENGTH))
        fill_this[zoltr_s + zobj.id.to_s] = zobj
      end
    end

    fill_this
  end # <- self.svc_branch_get

  private def decode_msgpack(klass, what2decode : Slice(UInt8))
    klass.from_msgpack(what2decode)
  end

  private def mget_zoids(redc_index : Int32, zoltr : String, zoids : Array(Int32), objs_store : Hash(String, (Cache2::Service | Cache2::Host | Cache2::HostGroup | Cache2::Trigger)))
    @redc[redc_index].mget(zoids).each_with_index do |zobj_s, ki|
      if zobj_s.is_a?(String)
        objs_store[zoltr + zoids[ki].to_s] = decode_msgpack(ZOLTR2TYPE[zoltr], (zobj_s.to_slice + ENCODE_PFX_LENGTH))
      end
    end
    return 1
  end

  private def mget_svcids_ret_deps(svcs : Array(Redis::RedisValue), fill_this : Hash(String, (Cache2::Service | Cache2::Host | Cache2::HostGroup | Cache2::Trigger)), assocs : NamedTuple(h: Hash(Int32, Bool), g: Hash(Int32, Bool), t: Hash(Int32, Bool))) : Array(Int32)
    svcs.each_with_object([] of Int32) do |svc_s, sids|
      if svc_s.is_a?(String) && svc_s.size > 4
        slcSerData = svc_s[4..-1].to_slice
        begin
          svc_o = Cache2::Service.from_msgpack(slcSerData)
        rescue ex
          puts "Exception #{ex.message} while processing MP: #{svc_s}"
          exit 1
        end
        fill_this["s" + svc_o.serviceid.to_s] = svc_o
        if (deps = svc_o.dependencies) && deps.is_a?(Array(Int32))
          sids.concat(deps)
        end
        begin
          assoc = Cache2::Assoc.from_msgpack(slcSerData)
        rescue
          # normal situation: no association/"zloid attribute" was defined for this service
        else
          zoltr = assoc.zloid[0].to_s
          if assocs[zoltr]?
            assocs[zoltr][assoc.zloid[1..-1].to_i] = true
          end
        end
      end
    end # <- @redc[redc_index].mget
  end   # <- private def mget_svcids_ret_deps
end     # <- class DruidF
