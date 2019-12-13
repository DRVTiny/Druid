require "redis"
require "benchmark"

class CondVar
  getter count : Int32

  def initialize
    @count = 0
  end

  def begin
    @count += 1
  end

  def end
    @count -= 1 if @count > 0
  end
end

class RecurseMgeter
  @redc_get : Redis
  @redc_scan : Redis
  @trigs : Array(String)
  @fut_trigs : Array(Redis::Future)

  macro reset_structs
		@trigs = [] of String
		@fut_trigs = [] of Redis::Future
	end

  def initialize(redis_db_n : Int32)
    @redc_get = Redis.new
    @redc_scan = Redis.new
    if redis_db_n > 0
      @redc_get.select(redis_db_n)
      @redc_scan.select(redis_db_n)
    end
    @trigs = [] of String
    @fut_trigs = [] of Redis::Future
  end

  def mget_all
    @trigs = [] of String
    @fut_trigs = [] of Redis::Future
    count = 0
    ch = Channel(Int32).new
    @redc_get.pipelined do |redc_pp|
      rcrs_mget(redc_pp, ch, pointerof(count))
      count.times { ch.receive }
      @fut_trigs.each do |ft|
        v = ft.value
        if v.is_a?(Array)
          v.each do |trg|
            if trg.is_a?(String)
              @trigs << trg
            end
          end
        end
      end
    end

    puts @trigs.size
  end

  private def rcrs_mget(pp : Redis::PipelineApi, ch : Channel(Int32), cnt : Pointer(Int32), next_id : Int32 = 0)
    rec = @redc_scan.scan(next_id)
    if (keys = rec[1]) && keys.is_a?(Array) && keys.size > 0
      cnt.value += 1
      spawn do
        @fut_trigs << pp.mget(keys).as(Redis::Future)
        ch.send(1)
      end
    end
    if (ink = rec[0]) && ink.is_a?(String)
      rcrs_mget(pp, ch, cnt, ink.to_i) unless ink == "0"
    else
      raise "Invalid rec[0] element"
    end
  end
end

mgeter = RecurseMgeter.new(5)
puts Benchmark.measure {
  mgeter.mget_all
}
