module Cequel

  module Model

    MissingAttributeError = Class.new(ArgumentError)
    UnknownAttributeError = Class.new(ArgumentError)
    RecordNotFound = Class.new(StandardError)

  end

end
