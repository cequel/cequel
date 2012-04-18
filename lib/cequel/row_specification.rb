module Cequel

  #
  # @private
  #
  class RowSpecification

    include Helpers

    def self.build(column_values)
      column_values.map { |column, value| new(column, value) }
    end

    attr_reader :column, :value

    def initialize(column, value)
      @column, @value = column, value
    end

    def cql
      case @value
      when DataSet
        subquery_cql
      when Array
        sanitize(
          "#{@column} IN (#{Array.new(@value.length) { '?' }.join(', ')})",
          *@value
        )
      else
        sanitize("#{@column} = ?", @value)
      end
    end

    private

    def subquery_cql
      values = values_from_subquery
      case values.length
      when 0
        raise EmptySubquery,
          "Unable to generate CQL row specification: subquery (#{@value.cql}) returned no results."
      when 1
        RowSpecification.new(@column, values.first).cql
      else
        RowSpecification.new(@column, values).cql
      end
    end

    def values_from_subquery
      results = @value.map do |row|
        if row.length > 1
          raise ArgumentError,
            "Subqueries must return a single row per column"
        end
        row.values.first
      end
    end

  end

end
