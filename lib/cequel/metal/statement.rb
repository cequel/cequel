module Cequel

  module Metal

    class Statement
      attr_reader :bind_vars, :length

      def initialize
        @cql, @bind_vars = [], []
      end

      def cql
        @cql.join
      end

      def prepend(cql, *bind_vars)
        @cql.unshift(cql)
        @bind_vars.unshift(*bind_vars)
      end

      def append(cql, *bind_vars)
        @cql << cql
        @bind_vars.concat(bind_vars)
        self
      end

      def args
        [cql, *bind_vars]
      end

    end

  end

end
