require 'active_model'

require 'cequel'
require 'cequel/model/associations'
require 'cequel/model/callbacks'
require 'cequel/model/class_internals'
require 'cequel/model/column'
require 'cequel/model/dirty'
require 'cequel/model/dynamic'
require 'cequel/model/errors'
require 'cequel/model/inheritable'
require 'cequel/model/instance_internals'
require 'cequel/model/local_association'
require 'cequel/model/mass_assignment_security'
require 'cequel/model/magic'
require 'cequel/model/naming'
require 'cequel/model/observer'
require 'cequel/model/persistence'
require 'cequel/model/properties'
require 'cequel/model/remote_association'
require 'cequel/model/scope'
require 'cequel/model/scoped'
require 'cequel/model/subclass_internals'
require 'cequel/model/timestamps'
require 'cequel/model/translation'
require 'cequel/model/validations'

if defined? Rails
  require 'cequel/model/railtie'
end

module Cequel

  #
  # This module adds Cassandra persistence to a class using Cequel.
  #
  module Model

    extend ActiveSupport::Concern
    extend ActiveModel::Observing::ClassMethods

    included do
      @_cequel = ClassInternals.new(self)

      include Properties
      include Persistence
      include Scoped
      include Naming
      include Callbacks
      include Validations
      include ActiveModel::Observing
      include Dirty
      include MassAssignmentSecurity
      include Associations
      extend Inheritable
      extend Magic

      include ActiveModel::Serializers::JSON
      include ActiveModel::Serializers::Xml

      extend Translation
    end

    def self.keyspace
      @keyspace ||= Cequel.connect(@configuration).tap do |keyspace|
        keyspace.logger = @logger if @logger
        keyspace.slowlog = @slowlog if @slowlog
        keyspace.slowlog_threshold = @slowlog_threshold if @slowlog_threshold
      end
    end

    def self.keyspace=(keyspace)
      @keyspace = keyspace
    end

    def self.configure(configuration)
      @configuration = configuration
    end

    def self.logger=(logger)
      @logger = logger
    end

    def self.slowlog=(slowlog)
      @slowlog = slowlog
    end

    def self.slowlog_threshold=(slowlog_threshold)
      @slowlog_threshold = slowlog_threshold
    end

    def initialize
      @_cequel = InstanceInternals.new(self)
    end

  end

end
