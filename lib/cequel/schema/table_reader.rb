# -*- encoding : utf-8 -*-
module Cequel
  module Schema
    #
    # A TableReader will query Cassandra's internal representation of a table's
    # schema, and build a {Table} instance exposing an object representation of
    # that schema
    #
    class TableReader
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
          check_for_compact_storage
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
      
      def check_for_compact_storage
        flags = table_data.fetch('flags', [])
        flags = ::Set.new(flags)
        
        if flags.include?('dense') || flags.include?('super') || !flags.include?('compound')
          table.compact_storage = true 
        end
      end

      def read_partition_keys
        partition_columns.sort_by { |c| c.fetch('position') }
          .each do |c|
            name = c.fetch('column_name').to_sym
            cql_type = Type.lookup_cql(c.fetch('type'))
            table.add_partition_key(name, cql_type)
          end
      end

      def read_clustering_columns
        cluster_columns.sort_by { |c| c.fetch('position') }
          .each do |c| 
            name = c.fetch('column_name').to_sym 
            cql_type = Type.lookup_cql(c.fetch('type'))
            clustering_order = c.fetch('clustering_order').to_sym
            table.add_clustering_column(name, cql_type, clustering_order)
          end  
      end

      COLLECTION_TYPE_PATTERN = /^(.+)<(.+)>/

      def read_data_columns 
        column_data.each do |result|
          name = result.fetch('column_name').to_sym
          
          column_type = result.fetch('type')
          m = COLLECTION_TYPE_PATTERN.match(column_type)
          
          if m.present? 
            composition_type = m[1]
            if composition_type != 'map' 
              cql_type = Type.lookup_cql(m[2])
              table.send("add_#{composition_type}", name, cql_type)
            else 
              composition_types = m[2].split(',').map(&:strip)
              key_type = composition_types.fetch(0)
              value_type = composition_types.fetch(1)
              table.send("add_#{composition_type}", name, key_type, value_type)
            end
          else 
            cql_type = Type.lookup_cql(column_type)
            
            table.add_data_column(name, cql_type)
          end
        end
      end

      def read_properties
        table_data.slice(*Table::STORAGE_PROPERTIES).each do |name, value|
          table.add_property(name, value)
        end        
      end

      def table_data
        return @table_data if defined? @table_data
        table_query = keyspace.execute(Cassandra::Cluster::Schema::Fetchers::V3_0_x::SELECT_TABLE, keyspace.name, table_name)
        @table_data = table_query.first.try(:to_hash)
      end

      def all_columns
        @all_columns ||=
          if table_data
            column_query = keyspace.execute(Cassandra::Cluster::Schema::Fetchers::V3_0_x::SELECT_TABLE_COLUMNS, keyspace.name, table_name)
            column_query.map(&:to_hash)
          end
      end

      def compact_value
        #TODO determine if this has test coverage and if it works or not in Cassandra 3
        @compact_value ||= all_columns.find do |column|
          column['type'] == 'compact_value'
        end || {}
      end

      def column_data
        @column_data ||= all_columns.select do |column|
          !column.key?('kind') || column.fetch('kind') == 'regular'
        end
      end

      def partition_columns
        @partition_columns ||= all_columns.select do |column|
          column.fetch('kind') == 'partition_key'
        end
      end

      def cluster_columns
        @cluster_columns ||= all_columns.select do |column|
          column.fetch('kind') == 'clustering'
        end
      end
    end
  end
end
