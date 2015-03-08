# -*- encoding : utf-8 -*-
module Cequel
  module Schema
    #
    # Creates a new table schema in the database
    #
    class TableWriter
      #
      # Creates a new table schema in the database given an object
      # representation of the schema to create
      #
      # @param (see #initialize)
      # @return (see #apply)
      #
      def self.apply(keyspace, table)
        new(keyspace, table).apply
      end

      #
      # @param keyspace [Keyspace] keyspace in which to create the table
      # @param table [Table] object representation of table schema
      # @private
      #
      def initialize(keyspace, table)
        @keyspace, @table = keyspace, table
      end
      private_class_method :new

      #
      # Create the table in the keyspace
      #
      # @return [void]
      #
      # @api private
      #
      def apply
        keyspace.execute(create_statement)
        index_statements.each { |statement| keyspace.execute(statement) }
      end

      protected

      attr_reader :keyspace, :table

      private

      def create_statement
        "CREATE TABLE #{table.name} (#{columns_cql}, #{keys_cql})".tap do |cql|
          properties = properties_cql
          cql << " WITH #{properties}" if properties
        end
      end

      def index_statements
        [].tap do |statements|
          table.data_columns.each do |column|
            if column.indexed?
              statements <<
                %(CREATE INDEX #{column.index_name} \
                  ON #{table.name} ("#{column.name}"))
            end
          end
        end
      end

      def columns_cql
        table.columns.map(&:to_cql).join(', ')
      end

      def key_columns_cql
        table.keys.map(&:to_cql).join(', ')
      end

      def keys_cql
        partition_cql = table.partition_key_columns
          .map { |key| %("#{key.name}") }.join(', ')
        if table.clustering_columns.any?
          nonpartition_cql =
            table.clustering_columns.map { |key| %("#{key.name}") }.join(', ')
          "PRIMARY KEY ((#{partition_cql}), #{nonpartition_cql})"
        else
          "PRIMARY KEY ((#{partition_cql}))"
        end
      end

      def properties_cql
        properties_fragments = table.properties
          .map { |_, property| property.to_cql }
        properties_fragments << 'COMPACT STORAGE' if table.compact_storage?
        if table.clustering_columns.any?
          clustering_fragment =
            table.clustering_columns.map(&:clustering_order_cql).join(',')
          properties_fragments <<
            "CLUSTERING ORDER BY (#{clustering_fragment})"
        end
        properties_fragments.join(' AND ') if properties_fragments.any?
      end
    end
  end
end
