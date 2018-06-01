require "json"
require "auto_msgpack"
require "redis"
require "kemal"
require "logger"
require "./macroDefinitions/json"
require "./macroDefinitions/msgpack"
require "./objectTypes/mixins/*"
require "./objectTypes/*"
require "./applicationClasses/DruidF"

module DruidWebApp
	DFLT_SERVICE_ID = 9594
	N_PROCS = 4
	log = Logger.new(STDERR)
	log.level = Logger::DEBUG	

	children_procs = [] of Process
	N_PROCS.times do
		children_procs << ( child_p = Process.fork do
			log.info("I am a Kemal worker process having pid #{Process.pid}")
			
			druid = DruidF.new
			
			before_all do |env|
				env.response.content_type = "application/json"
			end
		
			get "/service/:serviceid" do |env|
				if (svcid = env.params.url["serviceid"]) && svcid.is_a?(String) && svcid=~/^s?\d+$/
					druid.svc_branch_get((svcid[0] == 's' ? svcid[1..-1] : svcid).to_i).to_json
				else
					halt env, status_code: 404, response: %q({"error": "Wrong service identificator"})
				end
			rescue ex
				halt env, status_code: 503, response: {"error": "Unhandled exception #{ex.message}"}.to_json
			end
			
			sgnl_kemal_stop = Channel(Int32).new
			spawn do
				Kemal.run { |cfg| cfg.server.not_nil!.bind(reuse_port: true) }
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
