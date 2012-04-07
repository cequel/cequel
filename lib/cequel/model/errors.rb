module Cequel
  
  module Model

    Error = Class.new(Cequel::Error)
    RecordNotFound = Class.new(Error)
    RecordInvalid = Class.new(Error)

  end

end
