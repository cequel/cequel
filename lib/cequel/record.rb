require 'active_model'

require 'cequel'
require 'cequel/record/errors'
require 'cequel/record/schema'
require 'cequel/record/properties'
require 'cequel/record/collection'
require 'cequel/record/persistence'
require 'cequel/record/bulk_writes'
require 'cequel/record/record_set'
require 'cequel/record/bound'
require 'cequel/record/lazy_record_collection'
require 'cequel/record/scoped'
require 'cequel/record/secondary_indexes'
require 'cequel/record/associations'
require 'cequel/record/association_collection'
require 'cequel/record/belongs_to_association'
require 'cequel/record/has_many_association'
require 'cequel/record/mass_assignment'
require 'cequel/record/callbacks'
require 'cequel/record/validations'
require 'cequel/record/dirty'

require 'cequel/record'

if defined? Rails
  require 'cequel/record/railtie'
end

module Cequel

  module Record

    extend ActiveSupport::Concern
    extend Forwardable

    included do
      include Properties
      include Schema
      include Persistence
      include Associations
      include Scoped
      extend SecondaryIndexes
      include MassAssignment
      include Callbacks
      include Validations
      include Dirty
      extend ActiveModel::Naming
      include ActiveModel::Serializers::JSON
      include ActiveModel::Serializers::Xml

    end

    class <<self
      attr_accessor :connection

      def establish_connection(configuration)
        self.connection = Cequel.connect(configuration)
      end

    end

  end

end
