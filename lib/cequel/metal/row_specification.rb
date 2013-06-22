module Cequel

  module Metal

    #
    # @private
    #
    class RowSpecification

      def self.build(column_values)
        column_values.map { |column, value| new(column, value) }
      end

      attr_reader :column, :value

      def initialize(column, value)
        @column, @value = column, value
      end

      def cql
        case @value
        when Array
          if @value.length == 1
            ["#{@column} = ?", @value.first]
          else
            ["#{@column} IN (?)", @value]
          end
        else
          ["#{@column} = ?", @value]
        end
      end

    end

  end

end
