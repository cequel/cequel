# -*- encoding : utf-8 -*-
module Cequel
  module Schema
    #
    # A TableReader interprets table data from the cassandra driver into a table
    # descriptor (read: `Table`).
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

      class << self
        #
        # Read the schema defined in the database for a given table and return a
        # {Table} instance
        #
        # @param (see #initialize)
        # @return (see #read)
        #
        def read(keyspace, table_name)
          table_data = fetch_raw_keyspace(keyspace).table(table_name.to_s)
          (fail NoSuchTableError) if table_data.blank?

          new(table_data).call
        end

        protected

        def fetch_raw_keyspace(keyspace)
          cluster = keyspace.cluster
          cluster.refresh_schema

          (fail NoSuchKeyspaceError, "No such keyspace #{keyspace.name}") unless
            cluster.has_keyspace?(keyspace.name)

          cluster.keyspace(keyspace.name)
        end
      end

      #
      # @param keyspace [Metal::Keyspace] keyspace to read the table from
      # @param table_name [Symbol] name of the table to read
      # @private
      #
      def initialize(table_data)
        @table_data = table_data
        @table = Table.new(table_data.name,
                           Cassandra::MaterializedView === table_data)
      end

      #
      # Read table schema from the database
      #
      # @return [Table] object representation of table in the database, or
      #   `nil` if no table by given name exists
      #
      # @api private
      #
      def call
        return nil if table_data.blank?

        read_partition_keys
        read_clustering_columns
        read_indexes
        read_data_columns
        read_properties
        read_table_settings

        table
      end

      protected

      attr_reader :table_data, :table, :indexes

      def read_partition_keys
        table_data.partition_key.each do |k|
          table.add_column PartitionKey.new(k.name.to_sym, type(k.type))
        end

      end

      def read_clustering_columns
        table_data.clustering_columns
          .each do |c|
            table.add_column ClusteringColumn.new(c.name.to_sym, type(c.type), c.order)
          end
      end

      def read_indexes
        @indexes = if table_data.respond_to?(:each_index)
                     Hash[table_data.each_index.map{|i| [i.target, i.name]}]
                   else
                     # materialized view
                     {}
                   end
      end

      def read_data_columns
        ((table_data.each_column - table_data.partition_key) - table_data.clustering_columns)
          .each do |c|
            next if table.has_column?(c.name.to_sym)

            table.add_column interpret_column(c)
          end
      end

      def interpret_column(c)
        case c.type
        when Cassandra::Types::Simple
          DataColumn.new(c.name.to_sym, type(c.type), index_name(c))
        when Cassandra::Types::List
          List.new(c.name.to_sym, type(c.type.value_type))
        when Cassandra::Types::Set
          Set.new(c.name.to_sym, type(c.type.value_type))
        when Cassandra::Types::Map
          Map.new(c.name.to_sym, type(c.type.key_type), type(c.type.value_type))
        else
          fail "Unsupported type #{c.type.inspect}"
        end
      end

      @@prop_extractors = []
      def self.def_property(name,
                            option_method = name,
                            coercion = ->(val, _table_data){ val })

        @@prop_extractors << ->(table, table_data) {
          raw_prop_val = table_data.options.public_send(option_method)
          prop_val = coercion.call(raw_prop_val,table_data)

          table.add_property TableProperty.build(name, prop_val)
        }
      end

      def_property("bloom_filter_fp_chance")
      def_property("caching")
      def_property("comment")
      def_property("local_read_repair_chance")
      def_property("dclocal_read_repair_chance", :local_read_repair_chance)
      def_property("compression", :compression,
                   ->(comp, table_data) {
                     comp.clone.tap { |r|
                       r["chunk_length_kb"] ||= r["chunk_length_in_kb"] if r["chunk_length_in_kb"]
                       r["crc_check_chance"] ||= table_data.options.crc_check_chance
                     }
                   })
      def_property("compaction", :compaction_strategy,
                   ->(compaction_strategy, _table_data) {
                     compaction_strategy.options
                       .merge(class: compaction_strategy.class_name)
                   })
      def_property("gc_grace_seconds")
      def_property("read_repair_chance")
      def_property("replicate_on_write", :replicate_on_write?)

      def read_properties
        @@prop_extractors.each do |extractor|
          extractor.call(table, table_data)
        end
      end

      def read_table_settings
        table.compact_storage = table_data.options.compact_storage?
      end

      def type(type_info)
        ::Cequel::Type[type_info.kind]
      end

      def index_name(column_info)
        if idx_name = indexes[column_info.name]
          idx_name.to_sym
        else
          nil
        end
      end
    end
  end
end
