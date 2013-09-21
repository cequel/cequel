require 'stringio'

module Cequel

  module Schema

    class Table

      attr_reader :name,
                  :columns,
                  :partition_key_columns,
                  :clustering_columns,
                  :data_columns,
                  :properties
      attr_writer :compact_storage

      def initialize(name)
        @name = name
        @partition_key_columns, @clustering_columns, @data_columns = [], [], []
        @columns, @columns_by_name = [], {}
        @properties = ActiveSupport::HashWithIndifferentAccess.new
      end

      def add_key(name, type, clustering_order = nil)
        if @partition_key_columns.empty?
          unless clustering_order.nil?
            raise ArgumentError,
              "Can't set clustering order for partition key #{name}"
          end
          add_partition_key(name, type)
        else
          add_clustering_column(name, type, clustering_order)
        end
      end

      def add_partition_key(name, type)
        column = PartitionKey.new(name, type(type))
        @partition_key_columns << add_column(column)
      end

      def add_clustering_column(name, type, clustering_order = nil)
        column = ClusteringColumn.new(name, type(type), clustering_order)
        @clustering_columns << add_column(column)
      end

      def add_data_column(name, type, index_name)
        index_name = :"#{@name}_#{name}_idx" if index_name == true
        DataColumn.new(name, type(type), index_name).
          tap { |column| @data_columns << add_column(column) }
      end

      def add_list(name, type)
        @data_columns << add_column(List.new(name, type(type)))
      end

      def add_set(name, type)
        @data_columns << add_column(Set.new(name, type(type)))
      end

      def add_map(name, key_type, value_type)
        @data_columns <<
          add_column(Map.new(name, type(key_type), type(value_type)))
      end

      def add_property(name, value)
        @properties[name] = TableProperty.new(name, value)
      end

      def column(name)
        columns_by_name[name.to_sym]
      end

      def key_columns
        @partition_key_columns + @clustering_columns
      end

      def key_column_names
        key_columns.map { |key| key.name }
      end

      def partition_key(name)
        @partition_key_columns.find { |column| column.name == name }
      end

      def clustering_column(name)
        @clustering_columns.find { |column| column.name == name }
      end

      def data_column(name)
        name = name.to_sym
        @data_columns.find { |column| column.name == name }
      end

      def property(name)
        @properties[name].try(:value)
      end

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
        ::Cequel::Type[type]
      end

    end

  end

end
