require "json"
require "auto_msgpack"
require "redis"
require "./macroDefinitions/json"
require "./macroDefinitions/msgpack"
require "benchmark"
require "./objectTypes/Trigger"

TRIGS_DB        = 5
N_FIBERS_P2     = 5
N_FIBERS        = 1 << N_FIBERS_P2
ENCODE_PFX_SIZE = 4

puts Benchmark.measure {
  rM = Redis.new; rM.select(TRIGS_DB)

  triggerids = rM.keys("*")
  end_index = triggerids.size - 1
  batch_size = triggerids.size >> N_FIBERS_P2
  triggers = {} of UInt32 => Cache2::Trigger

  ch = Channel(Nil).new
  (N_FIBERS - 1).times do |i|
    spawn do
      ei = end_index - i * batch_size
      rS = Redis.new; rS.select(TRIGS_DB)
      rS.mget(triggerids[(ei - batch_size + 1)..ei]).each do |trg_s|
        next unless trg_s.is_a?(String) && trg_s.size > ENCODE_PFX_SIZE
        trg_o = Cache2::Trigger.from_msgpack(trg_s.to_slice + ENCODE_PFX_SIZE)
        triggers[trg_o.triggerid] = trg_o
      end
      ch.send(nil)
    end
  end
  rM.mget(triggerids[0..(batch_size + triggerids.size & (N_FIBERS - 1) - 1)]).each do |trg_s|
    next unless trg_s.is_a?(String) && trg_s.size > ENCODE_PFX_SIZE
    trg_o = Cache2::Trigger.from_msgpack(trg_s.to_slice + ENCODE_PFX_SIZE)
    triggers[trg_o.triggerid] = trg_o
  end
  (N_FIBERS - 1).times { ch.receive }
}
