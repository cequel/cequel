module Cequel

  module Schema

    class TableDSL < BasicObject

      def self.apply(table, &block)
        dsl = new(table)
        dsl.instance_eval(&block)
      end

      def initialize(table)
        @table = table
      end

      def partition_key(name, type)
        @table.add_partition_key(name, ::Cequel::Type[type])
      end

      def key(name, type, clustering_order = nil)
        @table.add_key(name, ::Cequel::Type[type], clustering_order)
      end

      def column(name, type, options = {})
        column = @table.add_column(name, ::Cequel::Type[type], options[:index])
      end

      def list(name, type)
        @table.add_list(name, ::Cequel::Type[type])
      end

      def set(name, type)
        @table.add_set(name, ::Cequel::Type[type])
      end

      def map(name, key_type, value_type)
        @table.add_map(
          name,
          ::Cequel::Type[key_type],
          ::Cequel::Type[value_type]
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
