# -*- encoding : utf-8 -*-
module Cequel
  module Metal
    #
    # DSL for the construction of a DELETE statement comprising multiple
    # operations (e.g. deleting a column value, deleting an element from a
    # list, etc.)
    #
    #
    # @note This class should not be instantiated directly
    # @see DataSet#delete
    # @see
    #   http://cassandra.apache.org/doc/cql3/CQL.html#deleteStmt
    #   CQL documentation for DELETE
    # @since 1.0.0
    #
    class Deleter < Writer
      #
      # Delete the entire row or rows matched by the data set
      #
      # @return [void]
      #
      def delete_row
        @delete_row = true
      end

      #
      # Delete specified columns
      #
      # @param columns [Symbol] column names to delete
      # @return [void]
      #
      def delete_columns(*columns)
        statements.concat(columns)
      end

      #
      # Remove elements from a list by position
      #
      # @param column [Symbol] name of list column
      # @param positions [Integer] positions in list from which to delete
      #   elements
      # @return [void]
      #
      def list_remove_at(column, *positions)
        statements
          .concat(positions.map { |position| %("#{column}"[#{position}]) })
      end

      #
      # Remote elements from a map by key
      #
      # @param column [Symbol] name of map column
      # @param keys [Object] keys to delete from map
      # @return [void]
      #
      def map_remove(column, *keys)
        statements.concat(keys.length.times.map { %("#{column}"[?]) })
        bind_vars.concat(keys)
      end

      private

      def write_to_statement(statement, options)
        if @delete_row
          statement.append("DELETE FROM #{table_name}")
        elsif statements.empty?
          fail ArgumentError, "No targets given for deletion!"
        else
          statement.append("DELETE ")
            .append(statements.join(','), *bind_vars)
            .append(" FROM #{table_name}")
        end
        statement.append(generate_upsert_options(options))
      end

      def empty?
        super && !@delete_row
      end
    end
  end
end
