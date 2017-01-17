# -*- encoding : utf-8 -*-
module Cequel
  module Metal
    #
    # Encapsulates a row specification (`WHERE` clause) constructed from a
    # column ane one or more values to match
    #
    # @api private
    #
    class RowSpecification
      #
      # Build one or more row specifications
      #
      # @param column_values [Hash] map of column name to value or values
      # @return [Array<RowSpecification>] collection of row specifications
      #
      def self.build(column_values)
        column_values.map { |column, value| new(column, value) }
      end

      # @return [Symbol] column name
      attr_reader :column
      # @return [Object, Array] value or values to match
      attr_reader :value

      #
      # @param column [Symbol] column name
      # @param value [Object,Array] value or values to match
      #
      def initialize(column, value)
        @column, @value = column, value
      end

      #
      # @return [String] row specification as CQL fragment
      #
      def cql
        value = if Enumerable === @value && @value.count == 1
                  @value.first
                else
                  @value
                end

        if Array === value
          ["#{@column} IN ?", value]
        else
          ["#{@column} = ?", value]
        end
      end
    end
  end
end
