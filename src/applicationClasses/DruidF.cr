require "auto_msgpack"
require "redis"
require "../macroDefinitions/json"
require "../macroDefinitions/msgpack"
require "../objectTypes/mixins/*"
require "../objectTypes/*"

class DruidF
  ENCODE_PFX                     = "{MP}"
  ENCODE_PFX_LENGTH              = ENCODE_PFX.size
  N_FIBERS_P2                    = 3
  N_FIBERS                       = 1 << N_FIBERS_P2
  N_MIN_SVC_OBJS_TO_USE_FIBERS   = N_FIBERS << 5
  N_MIN_ASSOC_OBJS_TO_USE_FIBERS = N_FIBERS * 50
  DFLT_ZOTYPE_DBN                = {t: 5, h: 6, g: 7, s: 8, c: 4}
  ZOTYPE2LTR                     = {host: "h", hostgroup: "g", trigger: "t", service: "s", group: "g"}
  ZOLTR2TYPE                     = {t: Cache2::Trigger, h: Cache2::Host, g: Cache2::HostGroup, s: Cache2::Service}
  ZOLTR2ID_ATTR                  = {t: "triggerid", h: "hostid", g: "groupid", s: "serviceid"}
  @@SVC_DEPS_TTL = 60

  @redc : Array(Redis)

  def initialize(@redis_db_n = DFLT_ZOTYPE_DBN, @svc_deps_ttl : Int32 = @@SVC_DEPS_TTL)
    puts "cache_ttl was set to #{@svc_deps_ttl} sec."
    @redc = (1..N_FIBERS).map { Redis.new }
  end

  def get_reload_ts(fl_have_to_select_dbn = true)
    @redc[0].select(@redis_db_n[:c]) if fl_have_to_select_dbn
    begin
      ts = @redc[0].get("reload_ts").try &.to_f64
    rescue
      ts = nil
    end
    return ts
  end

  def svc_branch_get(serviceid : Int32) : Hash(String, (Cache2::Service | Cache2::Host | Cache2::HostGroup | Cache2::Trigger))
    soid = "s#{serviceid}"
    @redc[0].select(@redis_db_n[:c])
    if cached_svc_deps = @redc[0].get(soid)
      ts_cached, deps_and_self = Tuple(Float64, NamedTuple(s: Array(Int32), h: Array(Int32), g: Array(Int32), t: Array(Int32))).from_msgpack(cached_svc_deps)
      ts_reload = get_reload_ts(false)

      if !ts_reload || ts_reload < ts_cached
        puts "Will use svc_branch_get_plain"
        zobjs = svc_branch_get_plain(deps_and_self)
        if (ts_reload = get_reload_ts(true)) && ts_reload > ts_cached
          puts "Cache was updated during zobjs retrieving. Have to redo from scratch"
          zobjs = nil
        end
      else
        puts "Cached data is older than last fullreload timestamp"
      end
    end

    unless zobjs
      puts "Will use svc_branch_get_tree_desc"
      zobjs, deps_and_self = svc_branch_get_tree_desc(serviceid)
      @redc[0].select(@redis_db_n[:c])
      if @redc[0].get(soid).is_a?(Nil)
        puts "Caching svc##{serviceid} deps info for #{@svc_deps_ttl} secs in redis_db=#{@redis_db_n[:c]}"
        @redc[0].setex(soid, @svc_deps_ttl, String.new({Time.new.epoch_f, deps_and_self}.to_msgpack))
      end
    end

    zobjs
  end

  private def svc_branch_get_plain(deps : NamedTuple(s: Array(Int32), h: Array(Int32), g: Array(Int32), t: Array(Int32))) : Hash(String, (Cache2::Service | Cache2::Host | Cache2::HostGroup | Cache2::Trigger))
    fill_this = {} of String => (Cache2::Service | Cache2::Host | Cache2::HostGroup | Cache2::Trigger)
    deps.each do |zoltr, zoids|
      next unless zoids.size > 0
      mget_zoids(zoltr, zoids, fill_this)
    end
    return fill_this
  end

  private def svc_branch_get_tree_desc(serviceid : Int32) : Tuple(Hash(String, (Cache2::Service | Cache2::Host | Cache2::HostGroup | Cache2::Trigger)), NamedTuple(s: Array(Int32), h: Array(Int32), g: Array(Int32), t: Array(Int32)))
    fill_this = {} of String => (Cache2::Service | Cache2::Host | Cache2::HostGroup | Cache2::Trigger)
    assocs = {h: {} of Int32 => Bool, g: {} of Int32 => Bool, t: {} of Int32 => Bool}
    zobj_ids = {s: [] of Int32, h: [] of Int32, g: [] of Int32, t: [] of Int32}
    # Select Redis Database Number which is used for Service objects storage
    @redc.each { |redc| redc.select(@redis_db_n[:s]) }
    # Empty "rcrs_get_deps" closure prototype to avoid  "read before assignment" compile-time exception while trying to do closure recurse call
    rcrs_get_deps = ->(x : Array(Int32)) { true }
    rcrs_get_deps = ->(serviceids : Array(Int32)) do
      nxt_lvl_sids = if serviceids.size >= N_MIN_SVC_OBJS_TO_USE_FIBERS
                       svcs = [] of String
                       n_objs_per_part = serviceids.size >> N_FIBERS_P2
                       end_index = serviceids.size - 1

                       ch_wait_get_svcs = Channel(Nil).new(N_FIBERS)
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
                       zobj_ids[:s] << svc_o.serviceid
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
      next unless zobj_ids[zoltr].replace(zoids_h.keys).size > 0
      mget_zoids(zoltr, zobj_ids[zoltr], fill_this)
    end

    {fill_this, zobj_ids}
  end # <- svc_branch_get_tree_desc

  private def decode_msgpack(klass, what2decode : Slice(UInt8))
    klass.from_msgpack(what2decode)
  end

  private def mget_zoids(zoltr : Symbol, zoids : Array(Int32), zobjs_decoded : Hash(String, (Cache2::Service | Cache2::Host | Cache2::HostGroup | Cache2::Trigger)))
    if zoids.size >= N_MIN_ASSOC_OBJS_TO_USE_FIBERS
      zobjs_s = [] of String
      n_objs_per_part = zoids.size >> N_FIBERS_P2
      end_index = zoids.size - 1

      ch_wait4fibers = Channel(Nil).new
      N_FIBERS.times do |i|
        spawn do
          ei = end_index - i * n_objs_per_part
          redc = @redc[i]
          redc.select(@redis_db_n[zoltr])
          redc.mget(zoids[(i == (N_FIBERS - 1) ? 0 : ei - n_objs_per_part + 1)..ei]).each do |s|
            zobjs_s << s if s.is_a?(String) && s.size > ENCODE_PFX_LENGTH
          end
          ch_wait4fibers.send(nil)
        end
      end
      N_FIBERS.times { ch_wait4fibers.receive }

      zobjs_s
    else
      @redc[0].select(@redis_db_n[zoltr])
      @redc[0].mget(zoids)
    end.each_with_index do |zobj_s, ki|
      next unless zobj_s.is_a?(String)
      zobj = decode_msgpack(ZOLTR2TYPE[zoltr], (zobj_s.to_slice + ENCODE_PFX_LENGTH))
      zobjs_decoded["#{zoltr}#{zobj.id}"] = zobj
    end
    return true
  end # <- mget_zoids()

  private def mget_svcids_ret_deps(svcs : Array(Redis::RedisValue), fill_this : Hash(String, (Cache2::Service | Cache2::Host | Cache2::HostGroup | Cache2::Trigger)), assocs : NamedTuple(h: Hash(Int32, Bool), g: Hash(Int32, Bool), t: Hash(Int32, Bool))) : Array(Int32)
    svcs.each_with_object([] of Int32) do |svc_s, sids|
      if svc_s.is_a?(String) && svc_s.size > ENCODE_PFX_LENGTH
        slcSerData = svc_s.to_slice + ENCODE_PFX_LENGTH
        begin
          svc_o = Cache2::Service.from_msgpack(slcSerData)
        rescue ex
          puts "Exception #{ex.message} while processing MP: #{svc_s}"
          exit 1
        end
        fill_this["s#{svc_o.serviceid}"] = svc_o
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
