# -*- encoding : utf-8 -*-
require 'delegate'

require 'active_support/core_ext'
require 'cql'

require 'cequel/errors'
require 'cequel/metal'
require 'cequel/schema'
require 'cequel/type'
require 'cequel/util'
require 'cequel/record'

#
# Cequel is a library providing robust data modeling and query building
# capabilities for Cassandra using CQL3.
#
# @see Cequel::Record Cequel::Record, an object-row mapper for CQL3
# @see Cequel::Metal Cequel::Metal, a query builder for CQL3 statements
# @see Cequel::Schema Cequel::Schema::Keyspace, which provides full read-write
#   access to the database schema defined in Cassandra
#
module Cequel
  #
  # Get a handle to a keyspace
  #
  # @param (see Metal::Keyspace#initialize)
  # @option (see Metal::Keyspace#initialize)
  # @return [Metal::Keyspace] a handle to a keyspace
  #
  def self.connect(configuration = nil)
    Metal::Keyspace.new(configuration || {})
  end

  #
  # Create a UUID
  #
  # @param timestamp [Time] timestamp to assign to the UUID
  # @return a UUID appropriate for use with Cequel
  #
  def self.uuid(timestamp = nil)
    if timestamp then timeuuid_generator.from_time(timestamp)
    else timeuuid_generator.next
    end
  end

  #
  # Determine if an object is a UUID
  #
  # @param object an object to check
  # @return [Boolean] true if the object is recognized by Cequel as a UUID
  #
  def self.uuid?(object)
    object.is_a?(Cql::Uuid)
  end

  def self.timeuuid_generator
    @timeuuid_generator ||= Cql::TimeUuid::Generator.new
  end
end
