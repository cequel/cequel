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
      def initialize(name)
        @name = name
        @partition_key_columns, @clustering_columns, @data_columns = [], [], []
        @columns, @columns_by_name = [], {}
        @properties = ActiveSupport::HashWithIndifferentAccess.new
      end

      #
      # Define a key column. If this is the first key column defined, it will
      # be a partition key; otherwise, it will be a clustering column.
      #
      # @param name [Symbol] the name of the column
      # @param type [Symbol,Type] the type for the column
      # @param clustering_order [:asc,:desc] whether rows should be in
      #   ascending or descending order by this column. Only meaningful for
      #   clustering columns.
      # @return [void]
      #
      # @see #add_partition_key
      #
      def add_key(name, type, clustering_order = nil)
        if @partition_key_columns.empty?
          unless clustering_order.nil?
            fail ArgumentError,
                 "Can't set clustering order for partition key #{name}"
          end
          add_partition_key(name, type)
        else
          add_clustering_column(name, type, clustering_order)
        end
      end

      #
      # Define a partition key for the table
      #
      # @param name [Symbol] the name of the column
      # @param type [Symbol,Type] the type for the column
      # @return [void]
      #
      def add_partition_key(name, type)
        PartitionKey.new(name, type(type)).tap do |column|
          @partition_key_columns << add_column(column)
        end
      end

      #
      # Define a clustering column for the table
      #
      # @param (see #add_key)
      # @return [void]
      #
      def add_clustering_column(name, type, clustering_order = nil)
        ClusteringColumn.new(name, type(type), clustering_order)
          .tap { |column| @clustering_columns << add_column(column) }
      end

      #
      # Define a data column on the table
      #
      # @param name [Symbol] name of the column
      # @param type [Type] type for the column
      # @param options [Options] options for the column
      # @option options [Boolean,Symbol] :index (nil) name of a secondary index
      #   to apply to the column, or `true` to infer an index name by
      #   convention
      # @return [void]
      #
      def add_data_column(name, type, options = {})
        options = {index: options} unless options.is_a?(Hash)
        index_name = options[:index]
        index_name = :"#{@name}_#{name}_idx" if index_name == true
        if type == :enum
          type = :int
        end
        DataColumn.new(name, type(type), index_name)
          .tap { |column| @data_columns << add_column(column) }
      end

      #
      # Define a list column on the table
      #
      # @param name [Symbol] name of the list
      # @param type [Symbol,Type] type of the list's elements
      # @return [void]
      #
      # @see List
      #
      def add_list(name, type)
        List.new(name, type(type)).tap do |column|
          @data_columns << add_column(column)
        end
      end

      #
      # Define a set column on the table
      #
      # @param name [Symbol] name of the set
      # @param type [Symbol,Type] type of the set's elements
      # @return [void]
      #
      # @see Set
      #
      def add_set(name, type)
        Set.new(name, type(type)).tap do |column|
          @data_columns << add_column(column)
        end
      end

      #
      # Define a map column on the table
      #
      # @param name [Symbol] name of the set
      # @param key_type [Symbol,Type] type of the map's keys
      # @param value_type [Symbol,Type] type of the map's values
      # @return [void]
      #
      # @see Map
      #
      def add_map(name, key_type, value_type)
        Map.new(name, type(key_type), type(value_type)).tap do |column|
          @data_columns << add_column(column)
        end
      end

      #
      # Define a storage property for the table
      #
      # @param name [Symbol] name of the property
      # @param value value for the property
      # @return [void]
      #
      # @see STORAGE_PROPERTIES List of storage property names
      # @see http://cassandra.apache.org/doc/cql3/CQL.html#createTableOptions
      #   list of CQL3 table storage properties
      #
      def add_property(name, value)
        TableProperty.build(name, value).tap do |property|
          @properties[name] = property
        end
      end

      #
      # @param name [Symbol] name of column to look up
      # @return [Column] column defined on table with given name
      #
      def column(name)
        columns_by_name[name.to_sym]
      end

      #
      # @return [Array<Symbol>] the names of all columns
      def column_names
        columns.map { |column| column.name }
      end

      #
      # @return [Array<Column>] all key columns (partition + clustering)
      #
      def key_columns
        @partition_key_columns + @clustering_columns
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
        @partition_key_columns.find { |column| column.name == name }
      end

      #
      # @param name [Symbol] name of clustering column to look up
      # @return [ClusteringColumn] clustering column with given name
      #
      def clustering_column(name)
        @clustering_columns.find { |column| column.name == name }
      end

      #
      # @param name [Symbol] name of data column to look up
      # @return [DataColumn,CollectionColumn] data column or collection column
      #   with given name
      #
      def data_column(name)
        name = name.to_sym
        @data_columns.find { |column| column.name == name }
      end

      #
      # @param name [Symbol] name of property to look up
      # @return [TableProperty] property as defined on table
      #
      def property(name)
        @properties[name].try(:value)
      end

      #
      # @return [Boolean] `true` if this table uses compact storage
      #
      def compact_storage?
        !!@compact_storage
      end

      protected

      attr_reader :columns_by_name

      private

      def add_column(column)
        columns << column
        columns_by_name[column.name] = column
      end

      def type(type)
        type = type.kind if type.respond_to?(:kind)

        ::Cequel::Type[type]
      end

    end
  end
end
