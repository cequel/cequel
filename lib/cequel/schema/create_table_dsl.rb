module Cequel
  module Schema
    #
    # Implements a DSL that can be used to define a table schema
    #
    # @see Keyspace#create_table
    #
    class CreateTableDSL < BasicObject
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
      # Add a partition key to the table
      #
      # @param (see Table#add_partition_key)
      # @return [void]
      #
      def partition_key(name, type)
        @table.add_partition_key(name, type)
      end

      #
      # Add a key to the table. If this is the first key in the table, it will
      # be a partition key; otherwise, it will be a clustering column
      #
      # @param (see Table#add_key)
      # @return [void]
      #
      # @see #partition_key
      #
      def key(name, type, clustering_order = nil)
        @table.add_key(name, type, clustering_order)
      end

      #
      # Add a data column to the table
      #
      # @param name [Symbol] name of the column
      # @param type [Type] type for the column
      # @param options [Options] options for the column
      # @option options [Boolean,Symbol] :index (nil) name of a secondary index
      #   to apply to the column, or `true` to infer an index name by convention
      # @return [void]
      #
      def column(name, type, options = {})
        @table.add_data_column(name, type, options[:index])
      end

      #
      # Add a list column to the table
      #
      # @param (see Table#add_list)
      # @return [void]
      #
      # @see List
      #
      def list(name, type)
        @table.add_list(name, type)
      end

      #
      # Add a set column to the table
      #
      # @param (see Table#add_set)
      # @return [void]
      #
      # @see Set
      #
      def set(name, type)
        @table.add_set(name, type)
      end

      #
      # Add a map column to the table
      #
      # @param (see Table#add_map)
      # @return [void]
      #
      # @see Map
      #
      def map(name, key_type, value_type)
        @table.add_map(name, key_type, value_type)
      end

      #
      # Add a storage property to the table
      #
      # @param (see Table#add_property)
      # @return [void]
      #
      # @see Map
      #
      def with(name, value)
        @table.add_property(name, value)
      end

      #
      # Direct that this table use “compact storage”. This is primarily useful
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
