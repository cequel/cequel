# -*- encoding : utf-8 -*-
module Cequel
  module Metal
    #
    # The Logger class encapsulates logging functionality for {Keyspace}.
    #
    # @api private
    #
    class Logger
      extend Forwardable
      # @return [::Logger] An instance of Logger from the standard library
      attr_reader :out
      # @return [Integer] The severity level for this logger
      attr_reader :severity
      # @return [Integer] Only log queries that take longer than threshold ms
      attr_accessor :threshold

      #
      # @param out [::Logger] An instance of Logger from the standard library
      # @param severity [Integer] The severity level for this logger
      # @param threshold [Integer] Only log queries that take longer than
      #   `threshold` ms
      #
      def initialize(out, severity, threshold = 0)
        @out, @severity, @threshold = out, severity, threshold
      end

      #
      # Log a CQL statement
      #
      # @param label [String] a logical label for this statement
      # @param timing [Integer] how long this statement took in ms
      # @param statement [String] the CQL statement to log
      # @param bind_vars [Array] bind variables for the CQL statement
      # @return [void]
      #
      def log(label, timing, statement, bind_vars)
        if timing >= threshold
          out.add(severity) do
            format(
              '%s (%dms) %s',
              label, timing, sanitize(statement, bind_vars)
            )
          end
        end
      end

      private

      def_delegator 'CassandraCQL::Statement', :sanitize
    end

    #
    # Logger for queries that resulted in an exception
    #
    class ExceptionLogger < Logger
      #
      # Log a CQL statement that resulted in an exception
      #
      # @param label [String] a logical label for this statement
      # @param statement [String] the CQL statement to log
      # @param bind_vars [Array] bind variables for the CQL statement
      # @return [void]
      #
      def log(label, statement, bind_vars)
        out.add(severity) do
          format('%s (ERROR) %s', label, sanitize(statement, bind_vars))
        end
      end
    end
  end
end
