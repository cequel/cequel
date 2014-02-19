# -*- encoding : utf-8 -*-
module Cequel
  module Schema
    #
    # Provides read/write access to the schema for a keyspace and the tables it
    # contains
    #
    # @deprecated These methods will be exposed directly on
    #   {Cequel::Metal::Keyspace} in a future version of Cequel
    #
    class Keyspace
      #
      # @param keyspace [Keyspace] the keyspace whose schema this object
      #   manipulates
      #
      # @api private
      #
      def initialize(keyspace)
        @keyspace = keyspace
      end

      #
      # Create this keyspace in the database
      #
      # @param options [Options] persistence options for this keyspace.
      # @option options [String] :class ("SimpleStrategy") the replication
      #   strategy to use for this keyspace
      # @option options [Integer] :replication_factor (1) the number of
      #   replicas that should exist for each piece of data
      # @return [void]
      #
      # @see
      #   http://cassandra.apache.org/doc/cql3/CQL.html#createKeyspaceStmt
      #   CQL3 CREATE KEYSPACE documentation
      #
      def create!(options = {})
        bare_connection =
          Metal::Keyspace.new(keyspace.configuration.except(:keyspace))

        options = options.symbolize_keys
        options[:class] ||= 'SimpleStrategy'
        if options[:class] == 'SimpleStrategy'
          options[:replication_factor] ||= 1
        end
        options_strs = options.map do |name, value|
          "'#{name}': #{Cequel::Type.quote(value)}"
        end

        bare_connection.execute(<<-CQL)
          CREATE KEYSPACE #{keyspace.name}
          WITH REPLICATION = {#{options_strs.join(', ')}}
        CQL
      end

      #
      # Drop this keyspace from the database
      #
      # @return [void]
      #
      # @see http://cassandra.apache.org/doc/cql3/CQL.html#dropKeyspaceStmt
      #   CQL3 DROP KEYSPACE documentation
      #
      def drop!
        keyspace.execute("DROP KEYSPACE #{keyspace.name}")
      end

      #
      # @param name [Symbol] name of the table to read
      # @return [Table] object representation of the table schema as it
      #   currently exists in the database
      #
      def read_table(name)
        TableReader.read(keyspace, name)
      end

      #
      # Create a table in the keyspace
      #
      # @param name [Symbol] name of the new table to create
      # @yield block evaluated in the context of a {CreateTableDSL}
      # @return [void]
      #
      # @example
      #   schema.create_table :posts do
      #     partition_key :blog_subdomain, :text
      #     key :id, :timeuuid
      #
      #     column :title, :text
      #     column :body, :text
      #     column :author_id, :uuid, :index => true
      #
      #     with :caching, :all
      #   end
      #
      # @see CreateTableDSL
      #
      def create_table(name, &block)
        table = Table.new(name)
        CreateTableDSL.apply(table, &block)
        TableWriter.apply(keyspace, table)
      end

      #
      # Make changes to an existing table in the keyspace
      #
      # @param name [Symbol] the name of the table to alter
      # @yield block evaluated in the context of an {UpdateTableDSL}
      # @return [void]
      #
      # @example
      #   schema.alter_table :posts do
      #     add_set :categories, :text
      #     rename_column :author_id, :author_uuid
      #     create_index :title
      #   end
      #
      # @see UpdateTableDSL
      #
      def alter_table(name, &block)
        updater = TableUpdater.apply(keyspace, name) do |updater|
          UpdateTableDSL.apply(updater, &block)
        end
      end

      #
      # Remove all data from this table. Truncating a table can be much slower
      # than simply iterating over its keys and issuing `DELETE` statements,
      # particularly if the table does not have many rows. Truncating is
      # equivalent to dropping a table and then recreating it
      #
      # @param name [Symbol] name of the table to truncate.
      # @return [void]
      #
      def truncate_table(name)
        keyspace.execute("TRUNCATE #{name}")
      end

      #
      # Drop this table from the keyspace
      #
      # @param name [Symbol] name of the table to drop
      # @return [void]
      #
      def drop_table(name)
        keyspace.execute("DROP TABLE #{name}")
      end

      #
      # Create or update a table to match a given schema structure. The desired
      # schema structure is defined by the directives given in the block; this
      # is then compared to the existing table in the database (if it is
      # defined at all), and then the table is created or altered accordingly.
      #
      # @param name [Symbol] name of the table to synchronize
      # @yield (see #create_table)
      # @return [void]
      # @raise (see TableSynchronizer#apply)
      #
      # @see #create_table Example of DSL usage
      #
      def sync_table(name, &block)
        existing = read_table(name)
        updated = Table.new(name)
        CreateTableDSL.apply(updated, &block)
        TableSynchronizer.apply(keyspace, existing, updated)
      end
      alias_method :synchronize_table, :sync_table

      protected

      attr_reader :keyspace
    end
  end
end
