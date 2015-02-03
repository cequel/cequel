# -*- encoding : utf-8 -*-
module Cequel
  module Schema
    #
    # Implements a DSL that can be used to define a table schema
    #
    # @see Keyspace#create_table
    #
    class CreateTableDSL < BasicObject
      extend ::Cequel::Util::Forwardable
      #
      # Evaluate `block` in the context of this DSL, and apply directives to
      # `table`
      #
      # @param table [Table] a table
      # @yield block evaluated in the context of the create table DSL
      # @return [void]
      #
      # @api private
      #
      def self.apply(table, &block)
        dsl = new(table)
        dsl.instance_eval(&block)
      end

      #
      # @param table [Table] table to apply directives to
      #
      # @api private
      #
      def initialize(table)
        @table = table
      end

      #
      # @!method partition_key(name, type)
      #   (see Cequel::Schema::Table#add_partition_key)
      #
      def_delegator :@table, :add_partition_key, :partition_key

      #
      # @!method key(name, type, clustering_order = nil)
      #   (see Cequel::Schema::Table#add_key)
      #
      def_delegator :@table, :add_key, :key

      #
      # @!method column(name, type, options = {})
      #   (see Cequel::Schema::Table#add_data_column)
      #
      def_delegator :@table, :add_data_column, :column

      #
      # @!method list(name, type)
      #   (see Cequel::Schema::Table#add_list)
      #
      def_delegator :@table, :add_list, :list

      #
      # @!method set(name, type)
      #   (see Cequel::Schema::Table#add_set)
      #
      def_delegator :@table, :add_set, :set

      #
      # @!method map(name, key_type, value_type)
      #   (see Cequel::Schema::Table#add_map)
      #
      def_delegator :@table, :add_map, :map

      #
      # @!method with(name, value)
      #   (see Cequel::Schema::Table#add_property)
      #
      def_delegator :@table, :add_property, :with

      #
      # Direct that this table use "compact storage". This is primarily useful
      # for backwards compatibility with legacy CQL2 table schemas.
      #
      # @return [void]
      #
      def compact_storage
        @table.compact_storage = true
      end
    end
  end
end
