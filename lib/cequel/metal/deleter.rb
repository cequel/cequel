module Cequel

  module Metal

    class Deleter < Writer

      def delete_row
        @delete_row = true
      end

      def delete_columns(*columns)
        statements.concat(columns)
      end

      def list_remove_at(column, *positions)
        statements.concat(positions.map { |position| "#{column}[#{position}]" })
      end

      def map_remove(column, *keys)
        statements.concat(keys.length.times.map { "#{column}[?]" })
        bind_vars.concat(keys)
      end

      private

      def write_to_statement(statement)
        if @delete_row
          statement.append("DELETE FROM #{table_name}")
        elsif statements.empty?
          raise ArgumentError, "No targets given for deletion!"
        else
          statement.append("DELETE ").
            append(statements.join(','), *bind_vars).
            append(" FROM #{table_name}")
        end
        statement.append(generate_upsert_options)
      end

      def empty?
        super && !@delete_row
      end

    end

  end

end
