require "json"
require "kemal"
require "app_name"
require "logger"
require "option_parser"
require "zabipi"
require "dotenv"
require "./applicationClasses/Druid"
require "./applicationClasses/DruidWebApp/LogHelper"
require "./monkeyPatches/cossack"
require "./monkeyPatches/redis"

ZAPI_CONFIG   = "/etc/zabbix/api/setenv.conf"

module DruidWebApp
  DFLT_SERVICE_ID       =     9594
  DFLT_BIND_TO_TCP_PORT = 3030_u16
  REDIS_CACHE_DBN       =        4
  N_PROCS               =        4
  TRIG_DESCR_CACHE_TTL  =       30 # seconds
  UNLOCK_SCRIPT = <<-EOSCRIPT
local lck_k, val_k = KEYS[1], KEYS[2]
local our_id, our_v, exp_p = ARGV[1], ARGV[2], ARGV[3]

local lck_v, aln_v
lck_v = redis.call("get", lck_k)
if lck_v == our_id then
  redis.call("del", lck_k)
  redis.call("set", val_k, our_v, "EX", exp_p)
  return nil
elseif not lck_v then
  if redis.call("exists", val_k) == 1 then
    return {1, redis.call("get", val_k)}
  else
    redis.call("set", val_k, our_v, "EX", exp_p)
    return {2, false}
  end
else
  aln_v = redis.call("get", val_k)
  if aln_v then
    return {3, aln_v}
  else
    return {4, lck_v}
  end
end  
EOSCRIPT
  TRIG_DESCR_ZAPI_TIMEOUT = 40
  WAIT_TRIG_VALUES_STEP_DUR = 0.05
  MAX_WAIT_CYCLES = TRIG_DESCR_ZAPI_TIMEOUT.to_f64 % WAIT_TRIG_VALUES_STEP_DUR
  
  svc_deps_ttl : Int32? = nil
  tcp_port : UInt16 = DFLT_BIND_TO_TCP_PORT
  setenv_conf = ZAPI_CONFIG
  log_file = "-"
  OptionParser.parse do |parser|
    parser.banner = "Usage: #{AppName.exec_name} [arguments]"
    parser.on("-C DEPS_CACHE_TTL", "--cache-ttl=DEPS_CACHE_TTL", "Service dependencies caching period") do |ttl|
      svc_deps_ttl = ttl.to_i.abs
    end
    parser.on("-p TCP_PORT_NUMBER", "--port TCP_PORT_NUMBER", "TCP port number to bind to") { |p| tcp_port = p.to_u16? || tcp_port }
    parser.on("-c ZAPI_CONFIG", "--setenv-config ZAPI_CONFIG", "Path to setenv config file") { |c| setenv_conf = c }
    parser.on("-l LOG_FILE", "--log-file	LOG_FILE", "Path to log file. Specify it as \"-\" if you need to log to STDERR (but this is already the default value)") do |lf|
      log_file = lf
    end
    parser.on("-h", "--help", "Show help message") { puts parser; exit(0) }
  end
  zenv = Dotenv.load(path: setenv_conf)
  children_procs = [] of Process
  # flush cache before use
  Redis.new(database: REDIS_CACHE_DBN).flushdb
  N_PROCS.times do
    children_procs << (child_p = Process.fork do
      log, fh_log = LogHelper.get_logger(log_file)
      log.info("Starting Kemal worker process")
      druid = if (t = svc_deps_ttl) && t > 0
                Druid.new(svc_deps_ttl: t)
              else
                Druid.new
              end
      zapi = Monitoring::Zabipi.new(zenv["ZBX_URL"], zenv["ZBX_LOGIN"], zenv["ZBX_PASS"])
      #			druid = (svc_deps_ttl && svc_deps_ttl > 0) ? Druid.new(svc_deps_ttl) : Druid.new
      before_all do |env|
        headers env, {
          "Content-Type"  => "application/json;charset=UTF-8",
          "X-Server-Time" => Time.local.to_unix.to_s,
        }
      end

      get "/service/:serviceid" do |env|
        #				puts "SERVICE: #{Process.pid}.#{Fiber.current.object_id}"
        if (svcid = env.params.url["serviceid"]) && svcid.is_a?(String) && svcid =~ /^s?\d+$/
          druid.svc_branch_get((svcid[0] == 's' ? svcid[1..-1] : svcid).to_i).to_json(env.response)
        else
          halt env, status_code: 404, response: %q({"error": "Wrong service identificator"})
        end
      rescue ex
        s = {"error": "Unhandled exception #{ex.message}"}.to_json
        halt env, status_code: 503, response: s
      end
      
      redc = Redis.new(database: REDIS_CACHE_DBN)
      sha1_unlock = redc.script_load(UNLOCK_SCRIPT)
      blck = ->(env : HTTP::Server::Context) {
        begin
          raise "no triggerids provided to me" unless tids = env.params.url["triggerids"]? || env.params.query["triggerids"]? || env.params.body["triggerids"]?.try &.as(String)
          triggerids = tids.split(/\s*,\s*/).map { |tid| tid.to_u32 rescue raise "triggerid must be positive integer" }
          log.debug("requested to get triggers: #{tids}")
          zapi_req_trigs = [] of UInt32
          wait_for_trigs = [] of UInt32
          trigs_descr = {} of UInt32 => String
          my_unique_id = {Process.pid, Fiber.current.object_id, Time.local.to_unix_ms, Random.new.hex[..5]}
                          .map {|e| e.is_a?(String) ? e : e.to_s }.join("/")
          ts_start = Time.local.to_unix_f
          redc.mget(triggerids.map {|tid| "t#{tid}_descr"}).each_with_index do |tdescr, i|
            tid = triggerids[i]
            # if we can get value directly from cache - do it!
            if tdescr
              trigs_descr[tid] = tdescr.to_s
            else
              if redc.set("t#{tid}_lock", my_unique_id, nx: true, ex: TRIG_DESCR_ZAPI_TIMEOUT)
                log.info("Acquired lock for t#{tid}")
                zapi_req_trigs << tid
              else
                log.warn("Someone locked update for t#{tid}, we have to wait")
                wait_for_trigs << tid
              end
            end
          end
          log.info(%Q<will use cached description values for triggerids: #{trigs_descr.keys}>) if trigs_descr.size > 0
          log.debug("time to mget triggers: #{((Time.local.to_unix_f - ts_start)*1e+6).to_i32} µs")
          if zapi_req_trigs.size > 0
            log.info("will send zabbix API request for triggerids: #{zapi_req_trigs}")
            zans = zapi.do("trigger.get", {"triggerids" => zapi_req_trigs, "expandDescription" => 1, "output" => ["description"]})
            zans.result.as_a.each do |r|
              tid, tdescr = r["triggerid"].as_s, r["description"].as_s
              if anomaly = redc.evalsha(sha1_unlock, ["t" + tid + "_lock", "t" + tid + "_descr"], [my_unique_id, tdescr, TRIG_DESCR_CACHE_TTL])
                if anomaly.is_a?(Array(Redis::RedisValue))
                  a_code, a_info = anomaly[0], anomaly[1].to_s
                  case a_code
                  when 1
                    log.warn("anomaly: lock not exists, but value is here")
                    trigs_descr[tid.to_u32] = a_info
                  when 2
                    log.warn("anomaly: lock not exists, but value does not exists too")
                    trigs_descr[tid.to_u32] = tdescr
                  when 3
                    log.warn("anomaly: lock was acquired by someone and value is here")
                    trigs_descr[tid.to_u32] = a_info
                  when 4
                    log.warn("anomaly: lock was acquired by someone (#{a_info}) but value is still not here")
                    trigs_descr[tid.to_u32] = tdescr
                  else
                    raise "unknown anomaly code: #{a_code}"
                  end
                else
                  raise "unlock anomaly detected, but can be resolved because of unknown anomaly type. dump follows: #{pp anomaly}"
                end
              else
                trigs_descr[tid.to_u32] = tdescr
              end
            end
          end
          
          if wait_for_trigs.size > 0
            cnt_wait = 0
            while wait_for_trigs.size > 0 && (cnt_wait += 1) <= MAX_WAIT_CYCLES
              log.info("triggerids to wait for value: #{wait_for_trigs}")
              sleep 0.05
              wait_for_trigs_new = [] of UInt32
              redc.mget(wait_for_trigs.map {|tid| "t#{tid}_descr"}).each_with_index do |tdescr, i|
                tid = wait_for_trigs[i]
                if tdescr
                  trigs_descr[tid] = tdescr.to_s
                else
                  wait_for_trigs_new << tid
                end
              end
              wait_for_trigs = wait_for_trigs_new
            end
            (wait_for_trigs.size > 0) && raise "timeout waiting for other process updating triggerids: #{wait_for_trigs}"
          end
#          log.info("will respond: << #{trigs_descr.to_json} >>")
          trigs_descr.to_json(env.response)
        rescue ex
          env.response.status_code = 503
          {"error": "#{ex.message}"}.to_json(env.response)
        end
      }

      get "/triggers/:triggerids", &blck
      get "/triggers", &blck
      post "/triggers", &blck

      sgnl_kemal_stop = Channel(Int32).new
      spawn do
        Kemal.config do |cfg|
          cfg.logger = KemaLoger.new(log, fh_log)
          log.info("We want to bind Kemal to tcp_port #{tcp_port}")
          cfg.port = tcp_port.to_i
        end
        Kemal.run do |cfg|
          cfg.server.not_nil!.bind_tcp(host: "localhost", port: tcp_port.to_i, reuse_port: true)
          #					log.info("But we was binded to tcp_port=#{cfg.server.not_nil!.port}")
        end
        sgnl_kemal_stop.send(1)
      end

      log.info("I am a child process with pid #{Process.pid}")
      Signal::TERM.trap do
        log.warn("Process #{Process.pid} received TERM signal, so we have to emergency cleanup and exit now")
        Kemal.stop
        exit(0)
      end
      exit_code = sgnl_kemal_stop.receive
      log.error("[#{Process.pid}] Ooops. How is that possible?")
      exit(1)
    end)
  end

  sgnl_sighup_rcvd = Channel(Int32).new
  {% for sgnl in %w(HUP TERM INT) %}
	Signal::{{sgnl.id}}.trap do
		sgnl_sighup_rcvd.send(1)
	end
	{% end %}
	
  log, _ = LogHelper.get_logger(log_file)
  log.info("I am a main process with pid #{Process.pid}. Send me HUP signal to terminate Kemal server gracefully")

  x = sgnl_sighup_rcvd.receive
  log.info("(some) signal received by master process #{Process.pid}. We have to terminate our children (by pid):\n\t[" + children_procs.map { |p| p.pid }.join(", ") + "]")

  children_procs.each do |child_p|
    child_pid = child_p.pid
    if child_p.terminated?
      log.warn("Child ##{child_pid} already terminated")
    else
      log.debug("Sending signal TERM (15) to process #{child_pid}")
      child_p.kill
      child_p.wait
    end
    log.error("Cant terminate child ##{child_pid}") unless child_p.terminated?
  end
end
