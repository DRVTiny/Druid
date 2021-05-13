require "log"
module DruidWebApp
  class LogHelper
    def self.configure_logger(file : (IO | String | Nil) = STDERR, log_level = Log::Severity::Debug)
      fh = 
        case file
        when IO
          file
        when String
          file == "-" ? STDERR : File.new(file, "a")
        else
          STDERR
        end
      
      
      log_fmt = Log::Formatter.new do |log_entry, io|
        log_sev = log_entry.severity.to_s.upcase
        ts = log_entry.timestamp
        io << [
          ts.to_s("%H:%M:%S"),
          ts.to_s("%d-%m-%Y"),
          log_sev,
          "pid=#{Process.pid}, fbr=#{Fiber.current.object_id}",
          log_entry.message
        ].join(" | ")
      end
      log_back = Log::IOBackend.new(fh, formatter: log_fmt)
      Log.setup("*", log_level, log_back)
      Log.progname = AppName.exec_name
      fh
    end
  end
  
  class KemaLoger < Kemal::LogHandler
    def initialize(@io : IO = STDERR)
    end
    
    def call(context : HTTP::Server::Context)
      Log.info { 
        {
          context.response.status_code,
          context.request.method,
          context.request.resource,
          elapsed_text(Time.measure { call_next(context) })
        }.join(' ')
      }
      context
    end
    
    def write(message : String)
      Log.info { message.chomp }
      @io
    end
  end
end
