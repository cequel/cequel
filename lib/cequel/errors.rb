module Cequel
  Error = Class.new(StandardError)
  EmptySubquery = Class.new(Error)
  InvalidSchemaMigration = Class.new(Error)
  MissingKeyError = Class.new(Error)
end
