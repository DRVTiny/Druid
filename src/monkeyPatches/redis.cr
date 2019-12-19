class Redis
  def database=(database_number : Number)
    raise Redis::Error.new("ERR database number could not be less than zero") if database_number < 0
    redis_dbn = database_number.is_a?(Int32) ? database_number : database_number.to_i32
    res = string_command(["SELECT", redis_dbn.to_s])
    if res == "OK"
      @database = redis_dbn
    else
      raise Redis::Error.new("ERR failed to switch to #{redis_dbn}: #{res}")
    end
    res  
  end
  
  def database
  	@database
  end
  
  def mget_pairs(keys : Array)
    string_array_command(concat(["MGET"], keys)).map_with_index do |red_val, ind|
      {keys[ind], red_val}
    end
  end 
  
  module CommandExecution
    # Command execution methods that return real values, not futures.

    module ValueOriented
    
      # Executes a Redis command and casts the response to the correct type.
      # This is an internal method.
      def string_array_or_any_command(request : Request) : Array(RedisValue) | RedisValue
        command(request).as(Array(RedisValue) | RedisValue)
      end
    end
  end
  
  module Commands
    def eval(script : String, keys = [] of RedisValue, args = [] of RedisValue)
      string_array_or_any_command(concat(["EVAL", script, keys.size.to_s], keys, args))
    end

    def evalsha(sha1, keys = [] of RedisValue, args = [] of RedisValue)
      string_array_or_any_command(concat(["EVALSHA", sha1.to_s, keys.size.to_s], keys, args))
    end
  end  
end
