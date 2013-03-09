require 'stringio'

module Cequel

  module Schema

    class Table

      attr_reader :name,
                  :partition_keys,
                  :nonpartition_keys,
                  :data_columns,
                  :properties
      attr_writer :compact_storage

      def initialize(name)
        @name = name
        @partition_keys, @nonpartition_keys, @data_columns = [], [], []
        @properties = ActiveSupport::HashWithIndifferentAccess.new
      end

      def add_key(name, type, clustering_order = nil)
        if @partition_keys.empty?
          unless clustering_order.nil?
            raise ArgumentError,
              "Can't set clustering order for partition key #{name}"
          end
          add_partition_key(name, type)
        else
          add_nonpartition_key(name, type, clustering_order)
        end
      end

      def add_partition_key(name, type)
        column = PartitionKey.new(name, type)
        @partition_keys << column
      end

      def add_nonpartition_key(name, type, clustering_order = nil)
        column = NonpartitionKey.new(name, type, clustering_order)
        @nonpartition_keys << column
      end

      def add_column(name, type, index_name)
        index_name = :"#{@name}_#{name}_idx" if index_name == true
        DataColumn.new(name, type, index_name).
          tap { |column| @data_columns << column }
      end

      def add_list(name, type)
        @data_columns << List.new(name, type)
      end

      def add_set(name, type)
        @data_columns << Set.new(name, type)
      end

      def add_map(name, key_type, value_type)
        @data_columns << Map.new(name, key_type, value_type)
      end

      def add_property(name, value)
        @properties[name] = TableProperty.new(name, value)
      end

      def columns
        @partition_keys + @nonpartition_keys + @data_columns
      end

      def column(name)
        columns.find { |column| column.name == name }
      end

      def partition_key(name)
        @partition_keys.find { |column| column.name == name }
      end

      def nonpartition_key(name)
        @nonpartition_keys.find { |column| column.name == name }
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

    end

  end

end
