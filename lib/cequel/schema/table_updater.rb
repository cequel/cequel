module Cequel

  module Schema

    class TableUpdater

      def self.apply(keyspace, table_name)
        new(keyspace, table_name).
          tap { |updater| yield updater if block_given? }.
          apply
      end

      def initialize(keyspace, table_name)
        @keyspace, @table_name = keyspace, table_name
        @statements = []
      end
      private_class_method :new

      def apply
        statements.each { |statement| keyspace.execute(statement) }
      end

      def add_column(name, type)
        add_data_column(Column.new(name, type))
      end

      def add_list(name, type)
        add_data_column(List.new(name, type))
      end

      def add_set(name, type)
        add_data_column(Set.new(name, type))
      end

      def add_map(name, key_type, value_type)
        add_data_column(Map.new(name, key_type, value_type))
      end

      def change_column(name, type)
        alter_table("ALTER #{name} TYPE #{type.cql_name}")
      end

      def rename_column(old_name, new_name)
        alter_table(%(RENAME "#{old_name}" TO "#{new_name}"))
      end

      def change_properties(options)
        properties = options.
          map { |name, value| TableProperty.new(name, value).to_cql }
        alter_table("WITH #{properties.join(' AND ')}")
      end

      def create_index(column, index_name)
        index_name ||= "#{table_name}_#{column}_idx"
        statements << "CREATE INDEX #{index_name} ON #{table_name} (#{column})"
      end

      def drop_index(index_name)
        statements << "DROP INDEX #{index_name}"
      end

      def add_data_column(column)
        add_column_statement(column)
      end

      protected
      attr_reader :keyspace, :table_name, :statements

      private

      def alter_table(statement)
        statements << "ALTER TABLE #{table_name} #{statement}"
      end

      def add_column_statement(column)
        alter_table("ADD #{column.to_cql}")
      end

    end

  end

end
