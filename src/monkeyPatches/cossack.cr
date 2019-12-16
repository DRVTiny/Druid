class Cossack::HTTPConnection
	def call(request : Request) : Response
		client = HTTP::Client.new(request.uri)
		client.connect_timeout = request.options.connect_timeout
		client.tls.verify_mode = OpenSSL::SSL::VerifyMode::NONE
		client.read_timeout = request.options.read_timeout

		http_response = client.exec(request.method, request.uri.to_s, request.headers, request.body)
		Response.new(http_response.status_code, http_response.headers, http_response.body)
	rescue err : IO::Timeout
		raise TimeoutError.new(err.message, cause: err)
	end
end
