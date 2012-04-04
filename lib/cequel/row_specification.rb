module Cequel

  #
  # @private
  #
  class RowSpecification

    include Helpers

    def self.build(column_values)
      column_values.map { |column, value| new(column, value) }
    end

    def initialize(column, value)
      @column, @value = column, value
    end
    private_class_method :new

    def to_cql
      if @value.is_a?(Array)
        sanitize(
          "#{@column} IN (#{Array.new(@value.length) { '?' }.join(', ')})",
          *@value
        )
      else
        sanitize("#{@column} = ?", @value)
      end
    end

  end

end
