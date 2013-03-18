module Cequel

  module Schema

    class Keyspace

      def initialize(keyspace)
        @keyspace = keyspace
      end

      def read_table(name)
        table_query = @keyspace.execute(<<-CQL, @keyspace.name, name)
          SELECT * FROM system.schema_columnfamilies
          WHERE keyspace_name = ? AND columnfamily_name = ?
        CQL
        table_data = table_query.first.try(:to_hash)
        if table_data
          column_query = @keyspace.execute(<<-CQL, @keyspace.name, name)
          SELECT * FROM system.schema_columns
          WHERE keyspace_name = ? AND columnfamily_name = ?
          CQL
          column_data = column_query.map(&:to_hash)
          TableReader.read(table_data, column_data)
        end
      end

      def create_table(name, &block)
        table = Table.new(name)
        CreateTableDSL.apply(table, &block)
        TableWriter.new(table).to_cql.each do |statement|
          @keyspace.execute(statement)
        end
      end

      def alter_table(name, &block)
        updater = TableUpdater.new(name)
        UpdateTableDSL.apply(updater, &block)
        updater.to_cql.each do |statement|
          @keyspace.execute(statement)
        end
      end

      def truncate_table(name)
        @keyspace.execute("TRUNCATE #{name}")
      end

      def drop_table(name)
        @keyspace.execute("DROP TABLE #{name}")
      end

      def sync_table(name, &block)
        existing = read_table(name)
        if existing
          updated = Table.new(name)
          CreateTableDSL.apply(updated, &block)
          updater = TableSynchronizer.new(existing, updated).updater
          updater.to_cql.each do |statement|
            @keyspace.execute(statement)
          end
        else
          create_table(name, &block)
        end
      end
      alias_method :synchronize_table, :sync_table

    end

  end

end
