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
      extend Util::Forwardable

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
      # @option options [Hash] :replication ({ class: "SimpleStrategy",
      #   replication_factor: 1 }) replication options for this keyspace
      # @option options [Boolean] :durable_writes (true) durable_writes
      #   option for the keyspace
      # @return [void]
      #
      # @see
      #   http://cassandra.apache.org/doc/cql3/CQL.html#createKeyspaceStmt
      #   CQL3 CREATE KEYSPACE documentation
      #
      def create!(options = {})
        bare_connection =
          Metal::Keyspace.new(keyspace.configuration.except(:keyspace))

        default_options = {
          replication: {
            class: "SimpleStrategy",
            replication_factor: 1
          },
          durable_writes: true
        }

        options = options.symbolize_keys
        options.reverse_merge!(keyspace.configuration.symbolize_keys)
        options.reverse_merge!(default_options)

        if options.key?(:class)
          options[:replication][:class] = options[:class]
          if options[:class] != 'SimpleStrategy'
            raise 'For strategy other than SimpleStrategy, please ' \
              'use the :replication option.'
          end
        end

        if options.key?(:replication_factor)
          options[:replication][:replication_factor] =
            options[:replication_factor]
        end

        replication_options_strs = options[:replication].map do |name, value|
          "'#{name}': #{Cequel::Type.quote(value)}"
        end

        bare_connection.execute(<<-CQL.strip_heredoc)
          CREATE KEYSPACE #{keyspace.name}
          WITH REPLICATION = {#{replication_options_strs.join(', ')}}
          AND durable_writes = #{options[:durable_writes]}
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
        keyspace.execute("DROP KEYSPACE #{keyspace.name}").tap do

          # If you execute a DROP KEYSPACE statement on a Cassandra::Session
          # with keyspace set to the one being dropped, then it will set
          # its keyspace to nil after the statement finishes. E.g.
          #   session.keyspace # => "cequel_test"
          #   session.execute("DROP KEYSPACE cequel_test")
          #   session.keyspace # => nil
          # This causes problems in the specs where we drop the test keyspace
          # and recreate it. Cequel::Record.connection.client's keyspace will
          # be set to nil after dropping the keyspace, but after creating it
          # again, it will still be set to nil. Easy fix is to just call
          # clear_active_connections! after dropping any keyspace.

          keyspace.clear_active_connections!
        end
      end

      # @return [Boolean] true if the keyspace exists
      def_delegator :keyspace, :exists?

      #
      # @param name [Symbol] name of the table to read
      # @return [Table] object representation of the table schema as it
      #   currently exists in the database
      #
      def read_table(name)
        TableReader.read(keyspace, name)
      end

      #
      # @param name [Symbol] name of the table to read
      # @return [TableReader] object
      #
      def get_table_reader(name)
        TableReader.get(keyspace, name)
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
        table = TableDescDsl.new(name).eval(&block)
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
      # @param exists [Boolean] if set to true, will drop only if exists (Cassandra 3.x)
      # @return [void]
      #
      def drop_table(name, exists: false)
        keyspace.execute("DROP TABLE #{'IF EXISTS ' if exists}#{name}")
      end

      #
      # Drop this materialized view from the keyspace
      #
      # @param name [Symbol] name of the materialized view to drop
      # @param exists [Boolean] if set to true, will drop only if exists (Cassandra 3.x)
      # @return [void]
      #
      def drop_materialized_view(name, exists: false)
        keyspace.execute("DROP MATERIALIZED VIEW #{'IF EXISTS ' if exists}#{name}")
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
        new_table_desc = TableDescDsl.new(name).eval(&block)
        patch = if has_table?(name)
                  existing_table_desc = read_table(name)
                  TableDiffer.new(existing_table_desc, new_table_desc).call
                else
                  TableWriter.new(new_table_desc) # close enough to a patch
                end

        patch.statements.each{|stmt| keyspace.execute(stmt) }
      end
      alias_method :synchronize_table, :sync_table


      # Returns true iff the specified table name exists in the keyspace.
      #
      def has_table?(table_name)
        !!read_table(table_name)
      end

      protected

      attr_reader :keyspace
    end
  end
end
