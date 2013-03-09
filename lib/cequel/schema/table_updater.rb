module Cequel

  module Schema

    class TableUpdater

      def initialize(name)
        @name = name
        @statements = []
      end

      def add_column(name, type)
        add_column_statement(Column.new(name, type))
      end

      def add_list(name, type)
        add_column_statement(List.new(name, type))
      end

      def add_set(name, type)
        add_column_statement(Set.new(name, type))
      end

      def add_map(name, key_type, value_type)
        add_column_statement(Map.new(name, key_type, value_type))
      end

      def change_column(name, type)
        alter_table("ALTER #{name} TYPE #{type.cql_name}")
      end

      def rename_column(old_name, new_name)
        alter_table("RENAME #{old_name} TO #{new_name}")
      end

      def change_properties(options)
        properties = options.
          map { |name, value| TableProperty.new(name, value).to_cql }
        alter_table("WITH #{properties.join(' AND ')}")
      end

      def create_index(column, index_name)
        index_name ||= "#{@name}_#{column}_idx"
        @statements << "CREATE INDEX #{index_name} ON #{@name} (#{column})"
      end

      def drop_index(index_name)
        @statements << "DROP INDEX #{index_name}"
      end

      def to_cql
        @statements
      end

      private

      def alter_table(statement)
        @statements << "ALTER TABLE #{@name} #{statement}"
      end

      def add_column_statement(column)
        alter_table("ADD #{column.to_cql}")
      end

    end

  end

end
