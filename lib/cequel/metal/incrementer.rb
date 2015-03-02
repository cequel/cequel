# -*- encoding : utf-8 -*-
module Cequel
  module Metal
    #
    # Encapsulates a counter `UPDATE` operation comprising multiple increment
    # or decrement operations
    #
    # @see DataSet#increment
    # @since 1.0.0
    #
    class Incrementer < Writer
      #
      # Increment one or more columns by given deltas
      #
      # @param data [Hash<Symbol,Integer>] map of column names to deltas
      # @return [void]
      #
      def increment(data)
        data.each_pair do |column_name, delta|
          operator = delta < 0 ? '-' : '+'
          statements << %("#{column_name}" = "#{column_name}" #{operator} ?)
          bind_vars << delta.abs
        end
      end

      #
      # Decrement one or more columns by given deltas
      #
      # @param data [Hash<Symbol,Integer>] map of column names to deltas
      # @return [void]
      #
      def decrement(data)
        increment(Hash[data.map { |column, count| [column, -count] }])
      end

      private

      def write_to_statement(statement, options)
        statement
          .append("UPDATE #{table_name}")
          .append(generate_upsert_options(options))
          .append(
            " SET " << statements.join(', '),
            *bind_vars
        )
      end
    end
  end
end
