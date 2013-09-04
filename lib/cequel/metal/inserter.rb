module Cequel

  module Metal

    class Inserter < Writer

      def initialize(data_set, options = {})
        @column_names = []
        super
      end

      def execute
        statement = Statement.new
        write_to_statement(statement)
        data_set.write(*statement.args)
      end

      def insert(data)
        data.each_pair do |column_name, value|
          column_names << column_name
          prepare_upsert_value(value) do |statement, *values|
            statements << statement
            bind_vars.concat(values)
          end
        end
      end

      private
      attr_reader :column_names

      def write_to_statement(statement)
        statement.append("INSERT INTO #{table_name}")
        statement.append(
          " (#{column_names.join(', ')}) VALUES (#{statements.join(', ')}) ",
          *bind_vars)
        statement.append(generate_upsert_options)
      end

    end

  end

end
