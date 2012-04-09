require 'active_model'

require 'cequel'
require 'cequel/model/callbacks'
require 'cequel/model/class_internals'
require 'cequel/model/column'
require 'cequel/model/dirty'
require 'cequel/model/errors'
require 'cequel/model/instance_internals'
require 'cequel/model/mass_assignment_security'
require 'cequel/model/naming'
require 'cequel/model/observing'
require 'cequel/model/persistence'
require 'cequel/model/properties'
require 'cequel/model/scope'
require 'cequel/model/scoped'
require 'cequel/model/translation'
require 'cequel/model/validations'

module Cequel

  #
  # This module adds Cassandra persistence to a class using Cequel.
  #
  module Model

    extend ActiveSupport::Concern

    included do
      @_cequel = ClassInternals.new(self)

      include Properties
      include Persistence
      include Scoped
      include Naming
      include Callbacks
      include Validations
      include Observing
      include Dirty
      include MassAssignmentSecurity

      include ActiveModel::Serializers::JSON
      include ActiveModel::Serializers::Xml

      extend Translation
    end

    def self.keyspace
      @keyspace
    end

    def self.keyspace=(keyspace)
      @keyspace = keyspace
    end

    def initialize
      @_cequel = InstanceInternals.new(self)
    end

  end

end
