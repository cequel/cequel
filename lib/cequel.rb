require 'active_support/core_ext'
require 'cassandra-cql/1.2'
require 'connection_pool'

require 'cequel/errors'
require 'cequel/metal'
require 'cequel/schema'
require 'cequel/type'
require 'cequel/util'
require 'cequel/record'

module Cequel
  def self.connect(configuration = nil)
    Metal::Keyspace.new(configuration || {})
  end
end
