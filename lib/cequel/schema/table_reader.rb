module Cequel

  module Schema

    class TableReader

      COMPOSITE_TYPE_PATTERN =
        /^org\.apache\.cassandra\.db\.marshal\.CompositeType\((.+)\)$/
      REVERSED_TYPE_PATTERN =
        /^org\.apache\.cassandra\.db\.marshal\.ReversedType\((.+)\)$/
      COLLECTION_TYPE_PATTERN =
        /^org\.apache\.cassandra\.db\.marshal\.(List|Set|Map)Type\((.+)\)$/

      STORAGE_PROPERTIES = %w[bloom_filter_fp_chance caching comment compaction
        compression dclocal_read_repair_chance gc_grace_seconds
        read_repair_chance replicate_on_write]

      attr_reader :table

      def self.read(keyspace, table_name)
        new(keyspace, table_name).read
      end

      def initialize(keyspace, table_name)
        @keyspace, @table_name = keyspace, table_name
        @table = Table.new(table_name.to_sym)
      end
      private_class_method(:new)

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

      def read_partition_keys
        validator = table_data['key_validator']
        types = parse_composite_types(validator) || [validator]
        JSON.parse(table_data['key_aliases']).zip(types) do |key_alias, type|
          name = key_alias.to_sym
          table.add_partition_key(key_alias.to_sym, Type.lookup_internal(type))
        end
      end

      def read_clustering_columns
        column_aliases = JSON.parse(table_data['column_aliases'])
        comparators = parse_composite_types(table_data['comparator'])
        unless comparators
          table.compact_storage = true
          return unless column_data.empty?
          column_aliases << :column1 if column_aliases.empty?
          comparators = [table_data['comparator']]
        end
        column_aliases.zip(comparators) do |column_alias, type|
          if REVERSED_TYPE_PATTERN =~ type
            type = $1
            clustering_order = :desc
          end
          table.add_clustering_column(
            column_alias.to_sym,
            Type.lookup_internal(type),
            clustering_order
          )
        end
      end

      def read_data_columns
        if column_data.empty?
          table.add_data_column(
            (table_data['value_alias'] || :value).to_sym,
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
        types = internal_types.map { |internal| Type.lookup_internal(internal) }
        table.__send__("add_#{collection_type}", name.to_sym, *types)
      end

      def read_properties
        table_data.slice(*STORAGE_PROPERTIES).each do |name, value|
          table.add_property(name, value)
        end
        compaction = JSON.parse(table_data['compaction_strategy_options']).
          symbolize_keys
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

      def column_data
        @column_data ||=
          if table_data
            column_query = keyspace.execute(<<-CQL, keyspace.name, table_name)
              SELECT * FROM system.schema_columns
              WHERE keyspace_name = ? AND columnfamily_name = ?
            CQL
            column_query.map(&:to_hash)
          end
      end

    end

  end

end
