# -*- encoding : utf-8 -*-
module Cequel
  module Metal
    #
    # Encapsulates a row specification (`WHERE` clause) specified by a CQL
    # string
    #
    # @api private
    #
    class CqlRowSpecification
      #
      # Build a new row specification
      #
      # @param (see #initialize)
      # @return [Array<CqlRowSpecification>]
      #
      def self.build(condition, bind_vars)
        [new(condition, bind_vars)]
      end

      #
      # Create a new row specification
      #
      # @param [String] condition CQL string representing condition
      # @param [Array] bind_vars Bind variables
      #
      def initialize(condition, bind_vars)
        @condition, @bind_vars = condition, bind_vars
      end

      #
      # CQL and bind variables for this condition
      #
      # @return [Array] CQL string followed by zero or more bind variables
      def cql
        [@condition, *@bind_vars]
      end
    end
  end
end
