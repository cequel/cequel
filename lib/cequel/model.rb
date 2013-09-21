require 'active_model'

require 'cequel'
require 'cequel/model/errors'
require 'cequel/model/schema'
require 'cequel/model/properties'
require 'cequel/model/collection'
require 'cequel/model/persistence'
require 'cequel/model/record_set'
require 'cequel/model/scoped'
require 'cequel/model/secondary_indexes'
require 'cequel/model/associations'
require 'cequel/model/association_collection'
require 'cequel/model/belongs_to_association'
require 'cequel/model/has_many_association'
require 'cequel/model/mass_assignment'
require 'cequel/model/callbacks'
require 'cequel/model/validations'
require 'cequel/model/dirty'

require 'cequel/model/base'

if defined? Rails
  require 'cequel/model/railtie'
end

module Cequel
  module Model
  end
end
