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
      # Return a {TableReader} instance
      #
      # @param (see #initialize)
      # @return [TableReader] object
      #
      def self.get(keyspace, table_name)
        new(keyspace, table_name)
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
          read_table_settings
          table
        end
      end

      #
      # Check if it is materialized view
      #
      # @return [boolean] true if it is materialized view
      #
      # @api private
      #
      def materialized_view?
        cluster.keyspace(keyspace.name)
          .has_materialized_view?(table_name.to_s)
      end

      protected

      attr_reader :keyspace, :table_name, :table

      private

      def read_partition_keys
        table_data.partition_key.each do |k|
          table.add_partition_key(k.name.to_sym, k.type)
        end

      end

      def read_clustering_columns
        table_data.clustering_columns.zip(table_data.clustering_order)
          .each do |c,o|
            table.add_clustering_column(c.name.to_sym, c.type, o)
          end
      end

      def read_data_columns
        indexes = Hash[table_data.each_index.map{|i| [i.target, i.name]}]

        ((table_data.each_column - table_data.partition_key) - table_data.clustering_columns)
          .each do |c|
            next if table.column(c.name.to_sym)
            case c.type
            when Cassandra::Types::Simple
              opts = if indexes[c.name]
                       {index: indexes[c.name].to_sym}
                     else
                       {}
                     end
              table.add_data_column(c.name.to_sym, c.type, opts)
            when Cassandra::Types::List
              table.add_list(c.name.to_sym, c.type.value_type)
            when Cassandra::Types::Set
              table.add_set(c.name.to_sym, c.type.value_type)
            when Cassandra::Types::Map
              table.add_map(c.name.to_sym, c.type.key_type, c.type.value_type)
            else
              fail "Unsupported type #{c.type.inspect}"
            end
          end
      end

      @@prop_extractors = []
      def self.def_property(name,
                            option_method = name,
                            coercion = ->(val, _table_data){ val })

        @@prop_extractors << ->(table, table_data) {
          raw_prop_val = table_data.options.public_send(option_method)
          prop_val = coercion.call(raw_prop_val,table_data)

          table.add_property(name, prop_val)
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

      def table_data
        @table_data ||= cluster.keyspace(keyspace.name)
          .table(table_name.to_s)
      end

      def cluster
        @cluster ||= begin
          cluster = keyspace.cluster
          cluster.refresh_schema

          fail(NoSuchKeyspaceError, "No such keyspace #{keyspace.name}") if
            !cluster.has_keyspace?(keyspace.name)

          cluster
        end
      end
    end
  end
end
