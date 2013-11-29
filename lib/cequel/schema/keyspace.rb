module Cequel

  module Schema

    class Keyspace

      def initialize(keyspace)
        @keyspace = keyspace
      end

      def create!(options = {})
        bare_connection =
          Metal::Keyspace.new(keyspace.configuration.except(:keyspace))

        options = options.symbolize_keys
        options[:class] ||= 'SimpleStrategy'
        options[:replication_factor] ||= 1 if options[:class] == 'SimpleStrategy'
        options_strs = options.map do |name, value|
          "'#{name}': #{CassandraCQL::Statement.quote(value)}"
        end

        bare_connection.execute(<<-CQL)
          CREATE KEYSPACE #{keyspace.name}
          WITH REPLICATION = {#{options_strs.join(', ')}}
        CQL
      end

      def drop!
        keyspace.execute("DROP KEYSPACE #{keyspace.name}")
      end

      def read_table(name)
        TableReader.read(keyspace, name)
      end

      def create_table(name, &block)
        table = Table.new(name)
        CreateTableDSL.apply(table, &block)
        TableWriter.apply(keyspace, table)
      end

      def alter_table(name, &block)
        updater = TableUpdater.apply(keyspace, name) do |updater|
          UpdateTableDSL.apply(updater, &block)
        end
      end

      def truncate_table(name)
        keyspace.execute("TRUNCATE #{name}")
      end

      def drop_table(name)
        keyspace.execute("DROP TABLE #{name}")
      end

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
