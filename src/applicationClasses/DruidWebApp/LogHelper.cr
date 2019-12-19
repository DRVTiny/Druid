module DruidWebApp
  class LogHelper
    def self.get_logger(file : (IO | String | Nil) = STDERR, log_level = Logger::DEBUG)
      fh = 
        case file
        when IO
          file
        when String
          file == "-" ? STDERR : File.new(file, "a")
        else
          STDERR
        end
      log_hndl = Logger.new(fh, log_level)
      log_hndl.progname = AppName.exec_name
      log_hndl.formatter = Logger::Formatter.new do |severity, datetime, progname, message, io|
        logsev = severity.unknown? ? "ANY" : severity.to_s
        io << [
          datetime.to_s("%H:%M:%S"),
          datetime.to_s("%d-%m-%Y"),
          logsev,
          "pid=#{Process.pid}, fbr=#{Fiber.current.object_id}",
          message
        ].join(" | ")       
      end
      {log_hndl, fh}
    end
  end
  
  class KemaLoger < Kemal::LogHandler
    def initialize(@log_hndl : Logger, @io : IO = STDERR)
    end
    
    def call(context : HTTP::Server::Context)
      @log_hndl.info({
        context.response.status_code,
        context.request.method,
        context.request.resource,
        elapsed_text(Time.measure { call_next(context) })
      }.join(' '))
      context
    end
    
    def write(message : String)
      @log_hndl.info(message.chomp)
      @io
    end
  end
end
