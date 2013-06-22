module Cequel

  module Metal

    #
    # @api private
    #
    class CqlRowSpecification

      def self.build(condition, bind_vars)
        [new(condition, bind_vars)]
      end

      def initialize(condition, bind_vars)
        @condition, @bind_vars = condition, bind_vars
      end

      def cql
        [@condition, *@bind_vars]
      end

    end

  end

end
