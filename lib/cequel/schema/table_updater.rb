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
        @statements << "ALTER #{name} TYPE #{type.cql_name}"
      end

      def rename_column(old_name, new_name)
        @statements << "RENAME #{old_name} TO #{new_name}"
      end

      def change_properties(options)
        properties = options.
          map { |name, value| TableProperty.new(name, value).to_cql }
        @statements << "WITH #{properties.join(' AND ')}"
      end

      def to_cql
        @statements.map do |statement|
          "ALTER TABLE #{@name} #{statement}"
        end
      end

      private

      def add_column_statement(column)
        @statements << "ADD #{column.to_cql}"
      end

    end

  end

end
