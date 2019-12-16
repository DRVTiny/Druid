require "msgpack"
require "redis"
require "../macroDefinitions/json"
require "../macroDefinitions/msgpack"
require "../objectTypes/mixins/*"
require "../objectTypes/*"

module Cache2
  alias Any = (Cache2::Service | Cache2::Host | Cache2::HostGroup | Cache2::Trigger)
end

class MPDecodingException < Exception
  def initialize(@msg_from_orig_except : String?, @what_cant_be_decoded : String)
    super(String.build do |sio|
      sio << "Exception catched: <<" << (@msg_from_orig_except || "!UNKNOWN!") << ">> When trying to decode msgpack string: <<" << @what_cant_be_decoded << ">> in hex: [" << hex_dump(@what_cant_be_decoded) << "]"
    end)
  end

  def hex_dump(s : String)
    s.to_slice.each_with_object(IO::Memory.new) { |b, sio| sio << b.to_s(16) }.to_s
  end
end

class Druid
  ENCODE_PFX                     = "{MP}"
  ENCODE_PFX_LENGTH              = ENCODE_PFX.size
  N_FIBERS_P2                    = 3
  N_FIBERS                       = 1 << N_FIBERS_P2
  N_MIN_SVCS_PER_FIBER           = N_FIBERS << 5
  N_MIN_ASSOC_OBJS_TO_USE_FIBERS = N_FIBERS * 50
  DFLT_ZOTYPE_DBN                = {t: 5, h: 6, g: 7, s: 8, c: 4}
  ZOTYPE2LTR                     = {host: "h", hostgroup: "g", trigger: "t", service: "s", group: "g"}
  ZOLTR2TYPE                     = {t: Cache2::Trigger, h: Cache2::Host, g: Cache2::HostGroup, s: Cache2::Service}
  ZOLTR2ID_ATTR                  = {t: "triggerid", h: "hostid", g: "groupid", s: "serviceid"}
  LAST_UPDATE_TS                 = "reload_ts"
  @@SVC_DEPS_TTL = 60
  @redcZObjs : Array(Redis?)
  @redcCache : Redis

  def initialize(@redis_db_n = DFLT_ZOTYPE_DBN, @svc_deps_ttl : Int32 = @@SVC_DEPS_TTL)
    # we can use lazy initialization of Redis connectors (initialize them only when needed, not in cosntructor)... but for what reason we may want to do this?
    @redcZObjs = Array(Redis?).new(N_FIBERS, nil)
    @redcCache = Redis.new
    @redcCache.select(@redis_db_n[:c])
  end

  def get_reload_ts
    ts = @redcCache.get(LAST_UPDATE_TS).try &.to_f64
  rescue
    nil
  end

  def svc_branch_get(serviceid : Int32) : Hash(String, Cache2::Any)
    soid = "s#{serviceid}_test"
    if serviceid > 0
      # if we can get the list of underlying services relative to the desired one (soid)...
      if cached_svc_deps = @redcCache.get(soid)
        ts_cached, deps_and_self = Tuple(Float64, NamedTuple(s: Array(Int32), h: Array(Int32), g: Array(Int32), t: Array(Int32))).from_msgpack(cached_svc_deps.to_slice + ENCODE_PFX_LENGTH)
        ts_reload = get_reload_ts

        if !ts_reload || ts_reload < ts_cached
          # TODO: normal logging instead of this shit. Will be something like logger.debug "Will use svc_branch_get_plain"
          puts "Will use svc_branch_get_plain because: " + (!ts_reload ? "!ts_reload" : "ts_reload < ts_cached")
          zobjs = svc_branch_get_plain(deps_and_self)
          if (ts_reload = get_reload_ts) && ts_reload > ts_cached
            puts "Very bad news: cache was updated/invalidated during zobjs retrieving. Have to redo from scratch"
            zobjs = nil
          end
        else
          puts "Cached data is older than last fullreload timestamp"
        end
      end
      # if we failed to get our services + associated objects set quickly, using cached data, then - use tree descending algorithm
      unless zobjs
        puts "Will use svc_branch_get_tree_desc"
        zobjs, deps_and_self = svc_branch_get_tree_desc(serviceid)
        if @redcCache.get(soid).is_a?(Nil)
          puts "Caching svc##{serviceid} deps info for #{@svc_deps_ttl} sec. in redis_db=#{@redis_db_n[:c]}, key=#{soid}"
          @redcCache.setex(soid, @svc_deps_ttl, ENCODE_PFX + String.new({Time.local.to_unix_f, deps_and_self}.to_msgpack))
        end
      end
    else
      zobjs = svc_get_all
    end

    zobjs
  end

  private def svc_branch_get_plain(deps : NamedTuple(s: Array(Int32), h: Array(Int32), g: Array(Int32), t: Array(Int32))) : Hash(String, Cache2::Any)
    fill_this = {} of String => Cache2::Any
    deps.each do |zoltr, zoids|
      next unless zoids.size > 0
      mget_zoids(zoltr, zoids, fill_this)
    end
    return fill_this
  end

  private def svc_get_all : Hash(String, Cache2::Any)
    fill_this = {} of String => Cache2::Any
    redcon = check_redc_before_use(0)

    # for zoltr, redis_dbn in @redis_db_n ...
    @redis_db_n.each do |zoltr, redbn|
      # skip cache redis_db
      next if zoltr == :c
      redcon.database = redbn
      zoids = redcon.keys("*").each_with_object([] of Int32) do |k, o|
        next unless k && k.is_a?(String) && k.size > 0
        begin
          o << k.to_i
        rescue
          next
        end
      end
      next unless zoids.size > 0
      mget_zoids(zoltr, zoids, fill_this, redis_database: redbn)
    end

    return fill_this
  end

  private def svc_branch_get_tree_desc(serviceid : Int32) : Tuple(Hash(String, (Cache2::Service | Cache2::Host | Cache2::HostGroup | Cache2::Trigger)), NamedTuple(s: Array(Int32), h: Array(Int32), g: Array(Int32), t: Array(Int32)))
    fill_this = {} of String => Cache2::Any
    assocs = {h: {} of Int32 => Nil, g: {} of Int32 => Nil, t: {} of Int32 => Nil}
    zobj_ids = {s: [] of Int32, h: [] of Int32, g: [] of Int32, t: [] of Int32}
    # Empty "rcrs_get_deps" closure prototype to avoid  "read before assignment" compile-time exception while trying to do closure recurse call
    rcrs_get_deps = ->(x : Array(Int32)) { true }
    rcrs_get_deps = ->(serviceids : Array(Int32)) do
      nxt_lvl_sids = [] of Int32
      mget_fiberized(serviceids).each do |serviceid, svc_s|
        unless svc_s.size > ENCODE_PFX_LENGTH
          puts "ERROR: s#{serviceid} length is #{svc_s.size}"
          next
        end

        svc_slice = svc_s.to_slice + ENCODE_PFX_LENGTH
        begin
          svc_o = Cache2::Service.from_msgpack(svc_slice)
        rescue ex
          raise MPDecodingException.new(ex.message, svc_s)
        end

        raise "serviceid: #{serviceid} != #{svc_o.serviceid}" unless serviceid == svc_o.serviceid

        if !(svc_o.triggerid || svc_o.dependencies)
          svc_o.dependencies = [] of Int32
        end

        zobj_ids[:s] << serviceid
        fill_this["s#{serviceid}"] = svc_o
        if (deps = svc_o.dependencies) && deps.is_a?(Array(Int32)) && deps.size > 0
          nxt_lvl_sids.concat(deps)
        end
        if triggerid = svc_o.triggerid
          assocs[:t][triggerid] = nil
        elsif (zloid = svc_o.zloid) && (zloid.size > 1)
          assocs[zloid[0..0]][zloid[1..-1].to_i] = nil
        end
      end

      if nxt_lvl_sids.size > 0
        rcrs_get_deps.call(nxt_lvl_sids) || raise "Unknown exception: previous call to <recursive get services> returns false"
        #            else
        #            	puts "This services seems to have no dependencies: " + serviceids.join(", ")
      end
      return true
    end # <- recursive get services

    rcrs_get_deps.call([serviceid])

    assocs.each do |zoltr, zoids_h|
      next unless zobj_ids[zoltr].replace(zoids_h.keys).size > 0
      #            	puts "No objs of type #{zoltr} found: #{zoids_h.keys}!"
      #            	next
      #           end
      mget_zoids(zoltr, zobj_ids[zoltr], fill_this)
    end

    {fill_this, zobj_ids}
  end # <- svc_branch_get_tree_desc

  private def decode_msgpack(klass, what2decode : Slice(UInt8))
    klass.from_msgpack(what2decode)
  end

  private def mget_fiberized(obj_ids : Array(Int32), obj_type = "s", min_objs_per_fiber = N_MIN_SVCS_PER_FIBER, n_fibers_p2 = N_FIBERS_P2) : Hash(Int32, String)
    unless (n_objs = obj_ids.size) > 0
      return {} of Int32 => String
    end
    db_n = @redis_db_n[obj_type[0..0]] || @redis_db_n[:s]
    #		puts "obj_type=#{obj_type}, db_n=#{db_n}"
    if n_objs >= min_objs_per_fiber
      #            puts "Fibers-get obj_ids: " + obj_ids.join(", ")
      objs = [] of Tuple(Int32, Redis::RedisValue)
      n_objs_per_part = n_objs >> n_fibers_p2

      n_fibers : Int32 = if n_objs_per_part < min_objs_per_fiber
        n_objs_per_part = Math.min(min_objs_per_fiber, n_objs)
        n_objs // n_objs_per_part
      else
        1 << n_fibers_p2
      end
      end_index = n_objs - 1

      ch_wait_get_objs = Channel(Array(Tuple(Int32, Redis::RedisValue))).new(n_fibers)
      n_fibers.times do |i|
        #            	redcon = check_redc_before_use( i, db_n )
        spawn do
          redcon = check_redc_before_use(i, db_n)
          ei = end_index - i * n_objs_per_part
          #                    @redcZObjs[i].select( db_n )
          #                    puts "fiber##{i}, db_n=#{db_n}, database=#{redcon.database}"
          #                    oids = obj_ids[(i == (n_fibers - 1) ? 0 : ei - n_objs_per_part + 1) .. ei]
          ch_wait_get_objs.send(redcon.mget_pairs(obj_ids[(i == (n_fibers - 1) ? 0 : ei - n_objs_per_part + 1)..ei]))
        end
      end
      n_fibers.times { objs.concat(ch_wait_get_objs.receive) }
      objs
    else
      #            puts "Short-get obj_ids: " + obj_ids.join(", ")
      check_redc_before_use(0, db_n).mget_pairs(obj_ids)
    end.each_with_object({} of Int32 => String) do |e, h|
      h[e[0]] = e[1].to_s
    end
  end # <- mget_fiberized

  private def check_redc_before_use(ind : Int32, redis_dbn : Number? = nil) : Redis
    redcon = @redcZObjs[ind]? || Redis.new
    begin
      raise "" unless redcon.ping == "PONG"
    rescue
      redcon = Redis.new
    end
    redcon.database = redis_dbn if redis_dbn
    @redcZObjs[ind] = redcon
  end

  private def mget_zoids(zoltr : Symbol, zoids : Array(Int32), zobjs_decoded : Hash(String, Cache2::Any), redis_database : Int32? = nil)
    mget_fiberized(zoids, obj_type: zoltr.to_s, min_objs_per_fiber: N_MIN_ASSOC_OBJS_TO_USE_FIBERS).each do |zobj_id, zobj_s|
      unless zobj_s.size > ENCODE_PFX_LENGTH
        puts "#{zoltr}#{zobj_id} class is #{zobj_s.class}, its length is #{zobj_s.is_a?(String) ? zobj_s.size : %q(N/A)}"
        next
      end

      begin
        zobj = decode_msgpack(ZOLTR2TYPE[zoltr], (zobj_s.to_slice + ENCODE_PFX_LENGTH))
      rescue ex
        raise MPDecodingException.new(ex.message, zobj_s)
      end
			if zobj.is_a?(Cache2::Service) && !( zobj.triggerid || zobj.dependencies )
				zobj.dependencies = [] of Int32
			end
      zobjs_decoded["#{zoltr}#{zobj.id}"] = zobj
    end
    return true
  end # <- mget_zoids()
end   # <- class Druid
