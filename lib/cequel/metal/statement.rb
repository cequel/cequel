module Cequel

  module Metal

    class Statement
      attr_reader :bind_vars

      def initialize
        @cql, @bind_vars = StringIO.new, []
      end

      def cql
        @cql.string
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
