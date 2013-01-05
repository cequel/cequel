require 'stringio'

module Cequel

  module Schema

    class Table

      attr_reader :name,
                  :partition_keys,
                  :nonpartition_keys,
                  :data_columns
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

      def create_cql
        create_statement = "CREATE TABLE #{@name} (#{columns_cql}, #{keys_cql})"
        properties = properties_cql
        create_statement << " WITH #{properties}" if properties
        [create_statement, *index_statements]
      end

      private

      def index_statements
        [].tap do |statements|
          @data_columns.each do |column|
            if column.indexed?
              statements <<
                "CREATE INDEX #{column.index_name} ON #{@name} (#{column.name})"
            end
          end
        end
      end

      def columns_cql
        columns.map(&:to_cql).join(', ')
      end

      def key_columns_cql
        @keys.map { |key| "#{key.name} #{key.type}" }.join(', ')
      end

      def keys_cql
        partition_cql = @partition_keys.map { |key| key.name }.join(', ')
        if @nonpartition_keys.any?
          nonpartition_cql =
            @nonpartition_keys.map { |key| key.name }.join(', ')
          "PRIMARY KEY ((#{partition_cql}), #{nonpartition_cql})"
        else
          "PRIMARY KEY ((#{partition_cql}))"
        end
      end

      def properties_cql
        properties_fragments = @properties.map { |_, property| property.to_cql }
        properties_fragments << 'COMPACT STORAGE' if @compact_storage
        if @nonpartition_keys.any?
          clustering_fragment =
            @nonpartition_keys.map(&:clustering_order_cql).join(',')
          properties_fragments << "CLUSTERING ORDER BY (#{clustering_fragment})"
        end
        properties_fragments.join(' AND ') if properties_fragments.any?
      end

    end

  end

end
