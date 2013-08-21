module Cequel

  module Metal

    class Incrementer < Writer

      def increment(data)
        data.each_pair do |column_name, delta|
          operator = delta < 0 ? '-' : '+'
          statements << "#{column_name} = #{column_name} #{operator} ?"
          bind_vars << delta.abs
        end
      end

      def decrement(data)
        increment(Hash[data.map { |column, count| [column, -count] }])
      end

      private

      def write_to_statement(statement)
        statement.
          append("UPDATE #{table_name}").
          append(generate_upsert_options).
          append(
            " SET " << statements.join(', '),
            *bind_vars
        )
      end

    end

  end

end
