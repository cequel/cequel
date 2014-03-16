# -*- encoding : utf-8 -*-
require 'cequel/metal/batch'
require 'cequel/metal/batch_manager'
require 'cequel/metal/cql_row_specification'
require 'cequel/metal/data_set'
require 'cequel/metal/logging'
require 'cequel/metal/keyspace'
require 'cequel/metal/request_logger'
require 'cequel/metal/row'
require 'cequel/metal/row_specification'
require 'cequel/metal/statement'
require 'cequel/metal/writer'
require 'cequel/metal/deleter'
require 'cequel/metal/incrementer'
require 'cequel/metal/inserter'
require 'cequel/metal/updater'

module Cequel
  #
  # The Cequel::Metal layer provides a low-level interface to the Cassandra
  # database. Most of the functionality is exposed via the DataSet class, which
  # encapsulates a table with optional filtering, and provides an interface for
  # constructing read and write queries. The Metal layer is not schema-aware,
  # and relies on the user to construct valid CQL queries.
  #
  # @see Keyspace
  # @see DataSet
  # @since 1.0.0
  #
  module Metal
  end
end
