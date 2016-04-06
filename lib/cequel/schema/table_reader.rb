# -*- encoding : utf-8 -*-
module Cequel
  module Schema
    #
    # A TableReader will query Cassandra's internal representation of a table's
    # schema, and build a {Table} instance exposing an object representation of
    # that schema
    #
    class TableReader
      COMPOSITE_TYPE_PATTERN =
        /^org\.apache\.cassandra\.db\.marshal\.CompositeType\((.+)\)$/
      REVERSED_TYPE_PATTERN =
        /^org\.apache\.cassandra\.db\.marshal\.ReversedType\((.+)\)$/
      COLLECTION_TYPE_PATTERN =
        /^org\.apache\.cassandra\.db\.marshal\.(List|Set|Map)Type\((.+)\)$/

      # @return [Table] object representation of the table defined in the
      #   database
      attr_reader :table

      #
      # Read the schema defined in the database for a given table and return a
      # {Table} instance
      #
      # @param (see #initialize)
      # @return (see #read)
      #
      def self.read(keyspace, table_name)
        new(keyspace, table_name).read
      end

      #
      # @param keyspace [Metal::Keyspace] keyspace to read the table from
      # @param table_name [Symbol] name of the table to read
      # @private
      #
      def initialize(keyspace, table_name)
        @keyspace, @table_name = keyspace, table_name
        @table = Table.new(table_name.to_sym)
      end
      private_class_method(:new)

      #
      # Read table schema from the database
      #
      # @return [Table] object representation of table in the database, or
      #   `nil` if no table by given name exists
      #
      # @api private
      #
      def read
        if table_data.present?
          read_partition_keys
          read_clustering_columns
          read_data_columns
          read_properties
          table
        end
      end

      protected

      attr_reader :keyspace, :table_name, :table

      private

      # XXX This gets a lot easier in Cassandra 2.0: all logical columns
      # (including keys) are returned from the `schema_columns` query, so
      # there's no need to jump through all these hoops to figure out what the
      # key columns look like.
      #
      # However, this approach works for both 1.2 and 2.0, so better to keep it
      # for now. It will be worth refactoring this code to take advantage of
      # 2.0's better interface in a future version of Cequel that targets 2.0+.
      def read_partition_keys
        validators = table_data['key_validator']
        types = parse_composite_types(validators) || [validators]
        columns = partition_columns.sort_by { |c| c['component_index'] }
          .map { |c| c['column_name'] }

        columns.zip(types) do |name, type|
          table.add_partition_key(name.to_sym, Type.lookup_internal(type))
        end
      end

      # XXX See comment on {read_partition_keys}
      def read_clustering_columns
        columns = cluster_columns.sort { |l, r| l['component_index'] <=> r['component_index'] }
          .map { |c| c['column_name'] }
        comparators = parse_composite_types(table_data['comparator'])
        unless comparators
          table.compact_storage = true
          return unless column_data.empty?
          columns << :column1 if cluster_columns.empty?
          comparators = [table_data['comparator']]
        end

        columns.zip(comparators) do |name, type|
          if REVERSED_TYPE_PATTERN =~ type
            type = $1
            clustering_order = :desc
          end
          table.add_clustering_column(
            name.to_sym,
            Type.lookup_internal(type),
            clustering_order
          )
        end
      end

      def read_data_columns
        if column_data.empty?
          table.add_data_column(
            (compact_value['column_name'] || :value).to_sym,
            Type.lookup_internal(table_data['default_validator']),
            false
          )
        else
          column_data.each do |result|
            if COLLECTION_TYPE_PATTERN =~ result['validator']
              read_collection_column(
                result['column_name'],
                $1.underscore,
                *$2.split(',')
              )
            else
              table.add_data_column(
                result['column_name'].to_sym,
                Type.lookup_internal(result['validator']),
                result['index_name'].try(:to_sym)
              )
            end
          end
        end
      end

      def read_collection_column(name, collection_type, *internal_types)
        types = internal_types
          .map { |internal| Type.lookup_internal(internal) }
        table.__send__("add_#{collection_type}", name.to_sym, *types)
      end

      def read_properties
        table_data.slice(*Table::STORAGE_PROPERTIES).each do |name, value|
          table.add_property(name, value)
        end
        compaction = JSON.parse(table_data['compaction_strategy_options'])
          .symbolize_keys
        compaction[:class] = table_data['compaction_strategy_class']
        table.add_property(:compaction, compaction)
        compression = JSON.parse(table_data['compression_parameters'])
        table.add_property(:compression, compression)
      end

      def parse_composite_types(type_string)
        if COMPOSITE_TYPE_PATTERN =~ type_string
          $1.split(',')
        end
      end

      def table_data
        return @table_data if defined? @table_data
        table_query = keyspace.execute(<<-CQL, keyspace.name, table_name)
              SELECT * FROM system.schema_columnfamilies
              WHERE keyspace_name = ? AND columnfamily_name = ?
        CQL
        @table_data = table_query.first.try(:to_hash)
      end

      def all_columns
        @all_columns ||=
          if table_data
            column_query = keyspace.execute(<<-CQL, keyspace.name, table_name)
              SELECT * FROM system.schema_columns
              WHERE keyspace_name = ? AND columnfamily_name = ?
            CQL
            column_query.map(&:to_hash)
          end
      end

      def compact_value
        @compact_value ||= all_columns.find do |column|
          column['type'] == 'compact_value'
        end || {}
      end

      def column_data
        @column_data ||= all_columns.select do |column|
          !column.key?('type') || column['type'] == 'regular'
        end
      end

      def partition_columns
        @partition_columns ||= all_columns.select do |column|
          column['type'] == 'partition_key'
        end
      end

      def cluster_columns
        @cluster_columns ||= all_columns.select do |column|
          column['type'] == 'clustering_key'
        end
      end
    end
  end
end
