require "redis"
require "benchmark"

class CondVar
	getter count : Int32
	def initialize
		@count=0
	end
	
	def begin
		@count+=1
	end
	
	def end
		@count-=1 if @count>0
	end
end

class RecurseMgeter
	@redc_get : Redis
	@redc_scan : Redis
	@trigs : Array(String)
	
	def initialize(redis_db_n : Int32)
		@redc_get = Redis.new
		@redc_scan = Redis.new
		if redis_db_n>0
			@redc_get.select(redis_db_n)
			@redc_scan.select(redis_db_n)
		end
		@trigs=[] of String
	end
	
	def mget_all
		@trigs=[] of String
		chan = Channel(Array(String)).new
		count = 0
		rcrs_mget(chan, pointerof(count))
		count.times do
			chan.receive.each {|s| @trigs << s}
		end
#		p @trigs
	end
	
    private def rcrs_mget (chan : Channel(Array(String)), cnt : Pointer(Int32), next_id : Int32 = 0)
        rec = @redc_scan.scan(next_id)
        if (keys=rec[1]) && keys.is_a?(Array) && keys.size>0
        	cnt.value+=1
        	spawn do
                res=[] of String
                @redc_get.mget(keys).each_with_index do |trg, i|
                    if trg.is_a?(String)
                        res << trg
                    end
                end
                chan.send(res)
            end
        end
        if (ink=rec[0]) && ink.is_a?(String)
            rcrs_mget(chan, cnt, ink.to_i) unless ink=="0"
        else
        	raise "Invalid rec[0] element"
        end
    end
end

mgeter = RecurseMgeter.new(5)
puts Benchmark.measure {
	mgeter.mget_all
}
