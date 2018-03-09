# -*- encoding : utf-8 -*-
require 'cequel/schema/column'
require 'cequel/schema/table_desc_dsl'
require 'cequel/schema/keyspace'
require 'cequel/schema/migration_validator'
require 'cequel/schema/table'
require 'cequel/schema/table_property'
require 'cequel/schema/table_reader'
require 'cequel/schema/table_differ'
require 'cequel/schema/patch'
require 'cequel/schema/table_updater'
require 'cequel/schema/table_writer'
require 'cequel/schema/update_table_dsl'

module Cequel
  #
  # The Schema module provides full read/write access to keyspace and table
  # schemas defined in Cassandra.
  #
  # @see Schema::Keyspace
  #
  # @since 1.0.0
  #
  module Schema
  end
end
