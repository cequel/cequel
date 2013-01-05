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

      def self.read(table_data, column_data)
        new(table_data, column_data).read
      end

      def initialize(table_data, column_data)
        @table_data, @column_data = table_data, column_data
        @table = Table.new(table_data['columnfamily_name'].to_sym)
      end
      private_class_method(:new)

      def read
        read_partition_keys
        read_nonpartition_keys
        read_data_columns
        read_properties
        @table
      end

      private

      def read_partition_keys
        validator = @table_data['key_validator']
        types = parse_composite_types(validator) || [validator]
        JSON.parse(@table_data['key_aliases']).zip(types) do |key_alias, type|
          name = key_alias.to_sym
          @table.add_partition_key(key_alias.to_sym, Type.lookup_internal(type))
        end
      end

      def read_nonpartition_keys
        column_aliases = JSON.parse(@table_data['column_aliases'])
        comparators = parse_composite_types(@table_data['comparator'])
        unless comparators
          @table.compact_storage = true
          comparators = [@table_data['comparator']]
        end
        column_aliases.zip(comparators) do |column_alias, type|
          if REVERSED_TYPE_PATTERN =~ type
            type = $1
            clustering_order = :desc
          end
          @table.add_nonpartition_key(
            column_alias.to_sym,
            Type.lookup_internal(type),
            clustering_order
          )
        end
      end

      def read_data_columns
        @column_data.each do |result|
          if COLLECTION_TYPE_PATTERN =~ result['validator']
            read_collection_column(
              result['column_name'],
              $1.underscore,
              *$2.split(',')
            )
          else
            @table.add_column(
              result['column_name'].to_sym,
              Type.lookup_internal(result['validator']),
              result['index_name'].try(:to_sym)
            )
          end
        end
      end

      def read_collection_column(name, collection_type, *internal_types)
        types = internal_types.map { |internal| Type.lookup_internal(internal) }
        @table.__send__("add_#{collection_type}", name.to_sym, *types)
      end

      def read_properties
        @table_data.slice(*STORAGE_PROPERTIES).each do |name, value|
          @table.add_property(name, value)
        end
        compaction = JSON.parse(@table_data['compaction_strategy_options']).
          symbolize_keys
        compaction[:class] = @table_data['compaction_strategy_class']
        @table.add_property(:compaction, compaction)
        compression = JSON.parse(@table_data['compression_parameters'])
        @table.add_property(:compression, compression)
      end

      def parse_composite_types(type_string)
        if COMPOSITE_TYPE_PATTERN =~ type_string
          $1.split(',')
        end
      end

    end

  end

end
