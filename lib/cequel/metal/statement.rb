# -*- encoding : utf-8 -*-
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
      # @return [Array] cassandra type hints for bind variables

      def initialize(cql_or_prepared='', bind_vars=[])
        cql, prepared = *case cql_or_prepared
                         when Cassandra::Statements::Prepared
                           [cql_or_prepared.cql, cql_or_prepared]
                         else
                           [cql_or_prepared.to_s, nil]
                         end

        @cql, @prepared, @bind_vars = cql, prepared, bind_vars
      end

      #
      # @return [String] CQL statement
      #
      def to_s
        @cql
      end
      alias_method :cql, :to_s

      # @return [Cassandra::Statements::Prepared] prepared version of this statement
      def prepare(keyspace)
        @prepared ||= keyspace.client.prepare(cql)
      end

      #
      # Add a CQL fragment with optional bind variables to the beginning of
      # the statement
      #
      # @param (see #append)
      # @return [void]
      #
      def prepend(cql, *bind_vars)
        @cql.prepend(cql)
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
        unless cql.nil?
          @cql << cql
          @bind_vars.concat(bind_vars)
        end
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
