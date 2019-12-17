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
end
