# -*- encoding : utf-8 -*-
require 'delegate'

require 'active_support'
require 'active_support/deprecation'
require 'active_support/core_ext'
require 'cassandra'

require 'cequel/errors'
require 'cequel/util'
require 'cequel/metal'
require 'cequel/schema'
require 'cequel/type'
require 'cequel/uuids'
require 'cequel/instrumentation'
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
  extend Cequel::Uuids
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
end
