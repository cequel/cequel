# -*- encoding : utf-8 -*-
module Cequel
  module Metal
    #
    # The Logger class encapsulates logging functionality for {Keyspace}.
    #
    # @api private
    #
    class RequestLogger
      extend Util::Forwardable
      # @return [::Logger] An instance of Logger that responds to methods for
      #   standard severity levels
      attr_accessor :logger
      # @return [Integer] Only log queries that take longer than threshold ms
      attr_accessor :slowlog_threshold

      def initialize
        self.slowlog_threshold = 2000
      end

      #
      # Log a CQL statement
      #
      # @param label [String] a logical label for this statement
      # @param statement [String] the CQL statement to log
      # @param bind_vars bind variables for the CQL statement
      # @return [void]
      #
      def log(label, statement, *bind_vars)
        return yield if logger.nil?

        response = nil
        begin
          time = Benchmark.ms { response = yield }
          generate_message = lambda do
            format_for_log(label, "#{time.round.to_i}ms", statement, bind_vars)
          end

          if time >= slowlog_threshold
            logger.warn(&generate_message)
          else
            logger.debug(&generate_message)
          end
        rescue Exception => e
          logger.error { format_for_log(label, 'ERROR', statement, bind_vars) }
          raise
        end
        response
      end

      private

      def format_for_log(label, timing, statement, bind_vars)
        bind_vars = bind_vars.map{|it| String === it ? limit_length(it) : it }
        format('%s (%s) %s', label, timing, sanitize(statement, bind_vars))
      end

      def limit_length(str)
        return str if str.length < 100

        str[0..25] + "..." + str[-25..-1]
      end

      def_delegator 'Cequel::Metal::Keyspace', :sanitize
      private :sanitize
    end
  end
end
