module Cequel

  module Schema

    class CreateTableDSL < BasicObject

      def self.apply(table, &block)
        dsl = new(table)
        dsl.instance_eval(&block)
      end

      def initialize(table)
        @table = table
      end

      def partition_key(name, type)
        @table.add_partition_key(name, type)
      end

      def key(name, type, clustering_order = nil)
        @table.add_key(name, type, clustering_order)
      end

      def column(name, type, options = {})
        column = @table.add_data_column(name, type, options[:index])
      end

      def list(name, type)
        @table.add_list(name, type)
      end

      def set(name, type)
        @table.add_set(name, type)
      end

      def map(name, key_type, value_type)
        @table.add_map(
          name,
          key_type,
          value_type
        )
      end

      def with(name, value)
        @table.add_property(name, value)
      end

      def compact_storage
        @table.compact_storage = true
      end

    end

  end

end
