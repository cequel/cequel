module Cequel

  module Schema


    class TableWriter

      def initialize(table)
        @table = table
      end

      def to_cql
        create_statement = "CREATE TABLE #{@table.name} (#{columns_cql}, #{keys_cql})"
        properties = properties_cql
        create_statement << " WITH #{properties}" if properties
        [create_statement, *index_statements]
      end

      private

      def index_statements
        [].tap do |statements|
          @table.data_columns.each do |column|
            if column.indexed?
              statements <<
                "CREATE INDEX #{column.index_name} ON #{@table.name} (#{column.name})"
            end
          end
        end
      end

      def columns_cql
        @table.columns.map(&:to_cql).join(', ')
      end

      def key_columns_cql
        @table.keys.map(&:to_cql).join(', ')
      end

      def keys_cql
        partition_cql = @table.partition_keys.map { |key| key.name }.join(', ')
        if @table.nonpartition_keys.any?
          nonpartition_cql =
            @table.nonpartition_keys.map { |key| key.name }.join(', ')
          "PRIMARY KEY ((#{partition_cql}), #{nonpartition_cql})"
        else
          "PRIMARY KEY ((#{partition_cql}))"
        end
      end

      def properties_cql
        properties_fragments = @table.properties.
          map { |_, property| property.to_cql }
        properties_fragments << 'COMPACT STORAGE' if @table.compact_storage?
        if @table.nonpartition_keys.any?
          clustering_fragment =
            @table.nonpartition_keys.map(&:clustering_order_cql).join(',')
          properties_fragments << "CLUSTERING ORDER BY (#{clustering_fragment})"
        end
        properties_fragments.join(' AND ') if properties_fragments.any?
      end

    end

  end

end
