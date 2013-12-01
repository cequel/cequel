module Cequel
  module Metal
    #
    # Builder for CQL statements. Contains a CQL string with bind substitutions
    # and a collection of bind variables
    #
    # @api private
    #
    class Statement
      # @return [Array] bind variables for CQL string
      attr_reader :bind_vars

      def initialize
        @cql, @bind_vars = [], []
      end

      #
      # @return [String] CQL statement
      #
      def cql
        @cql.join
      end

      #
      # Add a CQL fragment with optional bind variables to the beginning of
      # the statement
      #
      # @param (see #append)
      # @return [void]
      #
      def prepend(cql, *bind_vars)
        @cql.unshift(cql)
        @bind_vars.unshift(*bind_vars)
      end

      #
      # Add a CQL fragment with optional bind variables to the end of the
      # statement
      #
      # @param cql [String] CQL fragment
      # @param bind_vars [Object] zero or more bind variables
      # @return [void]
      #
      def append(cql, *bind_vars)
        @cql << cql
        @bind_vars.concat(bind_vars)
        self
      end

      #
      # @return [Array] this statement as an array of arguments to
      #   Keyspace#execute (CQL string followed by bind variables)
      #
      def args
        [cql, *bind_vars]
      end
    end
  end
end
