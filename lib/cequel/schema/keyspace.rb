module Cequel

  module Schema

    class Keyspace

      def initialize(keyspace)
        @keyspace = keyspace
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
