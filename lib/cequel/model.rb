require 'cequel'
require 'cequel/model/callbacks'
require 'cequel/model/class_internals'
require 'cequel/model/column'
require 'cequel/model/dirty'
require 'cequel/model/errors'
require 'cequel/model/instance_internals'
require 'cequel/model/persistence'
require 'cequel/model/properties'
require 'cequel/model/scope'
require 'cequel/model/scoped'

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
      include Callbacks
      include Dirty
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
