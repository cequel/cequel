module Cequel
  Error = Class.new(StandardError)
  InvalidSchemaMigration = Class.new(Error)
  MissingKeyError = Class.new(Error)
end
