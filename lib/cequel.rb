require 'active_support/core_ext'
require 'cassandra-cql'

require 'cequel/helpers'

require 'cequel/batch'
require 'cequel/errors'
require 'cequel/cql_row_specification'
require 'cequel/data_set'
require 'cequel/keyspace'
require 'cequel/row_specification'

module Cequel
  def self.connect(configuration)
    thrift_options = configuration[:thrift] || {}
    Keyspace.new(
      CassandraCQL::Database.new(
        configuration[:host] || configuration[:hosts],
        {:keyspace => configuration[:keyspace]},
        thrift_options.symbolize_keys
      )
    )
  end
end
