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

      # # @return [Array<Column>] all columns defined on the table
      # attr_reader :columns

      # @return [Array<PartitionKey>] partition key columns defined on the
      #   table
      # attr_reader :partition_key_columns

      # @return [Array<ClusteringColumn>] clustering columns defined on the
      #   table
      # attr_reader :clustering_columns

      # @return [Array<DataColumn,CollectionColumn>] data columns and
      #   collection columns defined on the table
      # attr_reader :data_columns

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
        # @partition_key_columns, @clustering_columns, @data_columns = [], [], []
        @properties = ActiveSupport::HashWithIndifferentAccess.new
      end

      # @return [Boolean] `true` when this table is a materialized view
      def materialized_view?
        @is_view
      end

      # Manages the columns by name that have been added to the
      # table. Uses the hash to create a unique value for this attribute.
      # The last one loaded in the source load will be the definition
      # of the column.
      def columns_by_name
        @columns_by_name ||= {}
      end

      # Accessor for the column values on the @columns_by_name hash
      # used to gain access to the column names.
      #
      def columns
        columns_by_name.values
      end

      # Manages the partition key columns that have been added to the
      # table. Uses the hash to create a unique value for this attribute.
      # Similar to columns, the last one loaded in the source load will be
      # the definition of the column.
      def partition_key_columns_hash
        @partition_key_columns_hash ||= {}
      end

      # Provides an accessor for the partition_key_columns as an array
      def partition_key_columns
        partition_key_columns_hash.values
      end

      # Manages the clustering key columns that have been added to the
      # table. Uses the hash to create a unique value for this attribute.
      # Similar to columns, the last one loaded in the source load will be
      # the definition of the column.
      def clustering_columns_hash
        @clustering_columns_hash ||= {}
      end

      # Provides an accessor for the clustering_key_columns as an array
      def clustering_columns
        clustering_columns_hash.values
      end

      # Manages the data key columns that have been added to the
      # table. Uses the hash to create a unique value for this attribute.
      # Similar to columns, the last one loaded in the source load will be
      # the definition of the column.
      def data_columns_hash
        @data_columns_hash ||= {}
      end

      # Provides an accessor for the data_key_columns as an array
      def data_columns
        data_columns_hash.values
      end

      # Add a column descriptor to this table descriptor.
      #
      # column_desc - Descriptor of column to add. Can be PartitionKey,
      #   ClusteringColumn, DataColumn, List, Set, or Map.
      #
      def add_column(column_desc)
        unless columns_by_name[column_desc.name]
          column_description_store(column_desc)[column_desc.name] = column_desc
          columns_by_name[column_desc.name]                       = column_desc
        end
      end

      # Stores the column description in the appropriate storage hash
      # for the column name and column type.
      def column_description_store(column_desc)
        value = {
          Cequel::Schema::PartitionKey     => partition_key_columns_hash,
          Cequel::Schema::ClusteringColumn => clustering_columns_hash
        }.try(:[], column_desc.class)
        value = data_columns_hash if value.nil?
        value
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
