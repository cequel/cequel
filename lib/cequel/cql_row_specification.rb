module Cequel

  #
  # @api private
  #
  class CqlRowSpecification

    include Helpers

    def self.build(condition, bind_vars)
      [new(condition, bind_vars)]
    end

    def initialize(condition, bind_vars)
      @condition, @bind_vars = condition, bind_vars
    end

    def cql
      sanitize(@condition, *@bind_vars)
    end

  end

end
