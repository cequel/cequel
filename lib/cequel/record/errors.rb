module Cequel

  module Record

    MissingAttributeError = Class.new(ArgumentError)
    UnknownAttributeError = Class.new(ArgumentError)
    RecordNotFound = Class.new(StandardError)
    InvalidRecordConfiguration = Class.new(StandardError)
    RecordInvalid = Class.new(StandardError)
    IllegalQuery = Class.new(StandardError)

  end

end
