require 'cequel'
require 'cequel/model/class_internals'
require 'cequel/model/column'
require 'cequel/model/instance_internals'
require 'cequel/model/properties'

module Cequel

  #
  # This module adds Cassandra persistence to a class using Cequel.
  #
  module Model

    extend ActiveSupport::Concern

    included do
      include Properties

      @_cequel = ClassInternals.new(self)
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
