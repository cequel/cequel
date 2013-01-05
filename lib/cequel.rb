require 'active_support/core_ext'
require 'cassandra-cql/1.2'
require 'connection_pool'

require 'cequel/batch'
require 'cequel/errors'
require 'cequel/cql_row_specification'
require 'cequel/data_set'
require 'cequel/keyspace'
require 'cequel/row_specification'
require 'cequel/schema'
require 'cequel/statement'
require 'cequel/type'

module Cequel
  def self.connect(configuration = nil)
    Keyspace.new(configuration || {})
  end
end
