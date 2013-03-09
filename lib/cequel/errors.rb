module Cequel
  Error = Class.new(StandardError)
  EmptySubquery = Class.new(Error)
  NotSupported = Class.new(Error)
end
