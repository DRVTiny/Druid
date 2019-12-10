require "json"
require "kemal"
require "logger"
require "option_parser"
require "zabipi"
require "dotenv"
require "./applicationClasses/Druid"
require "./monkeyPatches/cossack"

ZAPI_CONFIG = "/etc/zabbix/api/setenv.conf"
DFLT_APP_NAME = "druid_mp"

def get_app_name : String
	appName = Process.executable_path || DFLT_APP_NAME
	appName[((appName.rindex("/") || -1) + 1)..-1].gsub(/(?:^crystal-run-|\.tmp$)/,"")
end

module DruidWebApp

	DFLT_SERVICE_ID = 9594
	DFLT_BIND_TO_TCP_PORT = 3030_u16
	N_PROCS = 4
	
	log = Logger.new(STDERR)
	log.level = Logger::DEBUG
	
	svc_deps_ttl : Int32? = nil
	tcp_port : UInt16 = DFLT_BIND_TO_TCP_PORT
	setenv_conf = ZAPI_CONFIG
	OptionParser.parse do |parser|
		parser.banner = "Usage: #{get_app_name} [arguments]"
		parser.on("-C DEPS_CACHE_TTL", "--cache-ttl=DEPS_CACHE_TTL", "Service dependencies caching period") do |ttl|
			svc_deps_ttl = ttl.to_i.abs
		end
		parser.on("-p TCP_PORT_NUMBER", "--port TCP_PORT_NUMBER", 		 "TCP port number to bind to") {|p| tcp_port = p.to_u16? || tcp_port }
		parser.on("-c ZAPI_CONFIG",			"--setenv-config ZAPI_CONFIG", "Path to setenv config file") {|c| setenv_conf = c }
		parser.on("-h", 								"--help",											 "Show help message") { puts parser; exit(0) }
	end
  zenv = Dotenv.load(path: setenv_conf)
	children_procs = [] of Process
	N_PROCS.times do
		children_procs << ( child_p = Process.fork do
			log.info("I am a Kemal worker process having pid #{Process.pid}")
			druid = if (t = svc_deps_ttl) && t > 0
						Druid.new(svc_deps_ttl: t)
					else
						Druid.new
					end
			zapi = Monitoring::Zabipi.new(zenv["ZBX_URL"], zenv["ZBX_LOGIN"], zenv["ZBX_PASS"])
#			druid = (svc_deps_ttl && svc_deps_ttl > 0) ? Druid.new(svc_deps_ttl) : Druid.new
			before_all do |env|
				headers env, {
					"Content-Type" => "application/json",
					"X-Server-Time" => Time.local.to_unix.to_s
				}
			end
			
			get "/service/:serviceid" do |env|
#				puts "SERVICE: #{Process.pid}.#{Fiber.current.object_id}"
				if (svcid = env.params.url["serviceid"]) && svcid.is_a?(String) && svcid=~/^s?\d+$/
					druid.svc_branch_get((svcid[0] == 's' ? svcid[1..-1] : svcid).to_i).to_json(env.response)
				else
					halt env, status_code: 404, response: %q({"error": "Wrong service identificator"})
				end
			rescue ex
				s = {"error": "Unhandled exception #{ex.message}"}.to_json
				halt env, status_code: 503, response: s
			end
			
			blck = ->(env : HTTP::Server::Context) {
				begin
#					puts "TRIGGER: #{Process.pid}.#{Fiber.current.object_id}"
					env.response.content_type = "application/json;charset=UTF-8"
					if tids = env.params.url["triggerids"]? || env.params.query["triggerids"]? || env.params.body["triggerids"]?.try &.as(String)
						triggerids = tids.split(/\s*,\s*/).map {|tid| tid.to_u32 rescue raise "triggerid must be positive integer"}
						zans = zapi.do("trigger.get",{"triggerids" => triggerids, "expandDescription" => 1,"output" => ["description"]})
						zans.result.as_a.map {|r| {r["triggerid"].as_s.to_u32, r["description"]}}.to_h.to_json(env.response)
					else
						raise "no triggerids provided to me"
					end
				rescue ex
					env.response.status_code = 503
					{"error": "#{ex.message}"}.to_json(env.response)
				end
			}
			
			get  "/triggers/:triggerids", 	&blck
			get  "/triggers", 							&blck
			post "/triggers",               &blck
						
			sgnl_kemal_stop = Channel(Int32).new
			spawn do
				Kemal.config do |cfg| 
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
		end )	
	end

	sgnl_sighup_rcvd = Channel(Int32).new
	{% for sgnl in %w(HUP TERM INT) %}
	Signal::{{sgnl.id}}.trap do
		sgnl_sighup_rcvd.send(1)
	end
	{% end %}

	log.info( "I am a main process with pid #{Process.pid}. Send me HUP signal to terminate Kemal server gracefully" )

	x = sgnl_sighup_rcvd.receive
	log.info( "(some) signal received by master process #{Process.pid}. We have to terminate our children (by pid):\n\t[" + children_procs.map {|p| p.pid}.join(", ") + "]" )

	children_procs.each do |child_p|
		child_pid = child_p.pid
		if child_p.terminated?
			log.warn( "Child ##{child_pid} already terminated" )
		else
			log.debug("Sending signal TERM (15) to process #{child_pid}")
			child_p.kill
			child_p.wait
		end
		log.error( "Cant terminate child ##{child_pid}" ) unless child_p.terminated?
	end
end
