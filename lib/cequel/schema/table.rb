# -*- encoding : utf-8 -*-
require 'stringio'

module Cequel
  module Schema
    #
    # An object representation of a CQL3 table schema.
    #
    # @see Keyspace#read_table
    #
    class Table

      # @return [Symbol] the name of the table
      attr_reader :name

      # @return [Array<Column>] all columns defined on the table
      attr_reader :columns

      # @return [Array<PartitionKey>] partition key columns defined on the
      #   table
      attr_reader :partition_key_columns

      # @return [Array<ClusteringColumn>] clustering columns defined on the
      #   table
      attr_reader :clustering_columns

      # @return [Array<DataColumn,CollectionColumn>] data columns and
      #   collection columns defined on the table
      attr_reader :data_columns

      # @return [Hash] storage properties defined on the table
      attr_reader :properties

      # @return [Boolean] `true` if this table is configured with compact
      #   storage
      attr_writer :compact_storage

      #
      # @param name [Symbol] the name of the table
      # @api private
      #
      def initialize(name, is_view=false)
        @name = name.to_sym
        @is_view = is_view
        @partition_key_columns, @clustering_columns, @data_columns = [], [], []
        @columns, @columns_by_name = [], {}
        @properties = ActiveSupport::HashWithIndifferentAccess.new
      end

      # @return [Boolean] `true` when this table is a materialized view
      def materialized_view?
        @is_view
      end

      # Add a column descriptor to this table descriptor.
      #
      # column_desc - Descriptor of column to add. Can be PartitionKey,
      #   ClusteringColumn, DataColumn, List, Set, or Map.
      #
      def add_column(column_desc)
        column_flavor = case column_desc
                        when PartitionKey
                          @partition_key_columns
                        when ClusteringColumn
                          @clustering_columns
                        else
                          @data_columns
                        end

        column_flavor << column_desc
        columns << column_desc
        columns_by_name[column_desc.name] = column_desc
      end

      # Add a property to this table descriptor
      #
      # property_desc - A `TableProperty` describing one property of this table.
      #
      def add_property(property_desc)
        properties[property_desc.name] = property_desc
      end

      #
      #
      # @param name [Symbol] name of column to look up
      # @return [Column] column defined on table with given name
      #
      def column(name)
        columns_by_name[name.to_sym]
      end

      # Returns true iff this table has the specified column name.
      #
      def has_column?(name)
        columns_by_name.has_key?(name.to_sym)
      end

      #
      # @return [Array<Symbol>] the names of all columns
      def column_names
        columns_by_name.keys
      end

      #
      # @return [Array<Column>] all key columns (partition + clustering)
      #
      def key_columns
        partition_key_columns + clustering_columns
      end

      #
      # @return [Array<Symbol>] names of all key columns (partition +
      #   clustering)
      #
      def key_column_names
        key_columns.map { |key| key.name }
      end

      #
      # @return [Integer] total number of key columns
      #
      def key_column_count
        key_columns.length
      end

      #
      # @return [Array<Symbol>] names of partition key columns
      #
      def partition_key_column_names
        partition_key_columns.map { |key| key.name }
      end

      #
      # @return [Integer] number of partition key columns
      #
      def partition_key_column_count
        partition_key_columns.length
      end

      # Returns true iff this table descriptor currently has at least one
      # partition key defined.
      def has_partition_key?
        partition_key_columns.any?
      end

      #
      # @return [Array<Symbol>] names of clustering columns
      #
      def clustering_column_names
        clustering_columns.map { |key| key.name }
      end

      #
      # @return [Integer] number of clustering columns
      #
      def clustering_column_count
        clustering_columns.length
      end

      #
      # @param name [Symbol] name of partition key column to look up
      # @return [PartitionKey] partition key column with given name
      #
      def partition_key(name)
        partition_key_columns.find { |column| column.name == name }
      end

      #
      # @param name [Symbol] name of clustering column to look up
      # @return [ClusteringColumn] clustering column with given name
      #
      def clustering_column(name)
        clustering_columns.find { |column| column.name == name }
      end

      #
      # @param name [Symbol] name of data column to look up
      # @return [DataColumn,CollectionColumn] data column or collection column
      #   with given name
      #
      def data_column(name)
        name = name.to_sym
        data_columns.find { |column| column.name == name }
      end

      #
      # @param name [Symbol] name of property to look up
      # @return [TableProperty] property as defined on table
      #
      def property(name)
        properties.fetch(name, null_table_property).value
      end

      #
      # @return [Boolean] `true` if this table uses compact storage
      #
      def compact_storage?
        !!@compact_storage
      end

      protected

      attr_reader :columns_by_name

      def type(type)
        type = type.kind if type.respond_to?(:kind)

        ::Cequel::Type[type]
      end

      def null_table_property
        @@null_table_property ||= Class.new do
          def value; nil; end
          def name; nil; end
        end.new
      end
    end
  end
end
