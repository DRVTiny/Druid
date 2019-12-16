require "auto_msgpack"
require "redis"
require "../macroDefinitions/json"
require "../macroDefinitions/msgpack"
require "../objectTypes/mixins/*"
require "../objectTypes/*"

module Cache2
  alias Any = (Cache2::Service | Cache2::Host | Cache2::HostGroup | Cache2::Trigger)
end

class MyApp
  def initialize(@obj : Cache2::Any)
    puts obj.name
  end
end

a = MyApp.new(Cache2::HostGroup.new({"name" => "Group", "groupid" => 0}))
# a : Cache2::Any = Cache2::HostGroup.new({"name"=>"Group", "groupid"=>0})
