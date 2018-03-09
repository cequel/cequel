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
      # @param statement [String,Statement,Batch] the CQL statement to log
      # @return [void]
      #
      def log(label, statement)
        return yield if logger.nil?

        response = nil
        begin
          time = Benchmark.ms { response = yield }
          generate_message = lambda do
            format_for_log(label, "#{time.round.to_i}ms", statement)
          end

          if time >= slowlog_threshold
            logger.warn(&generate_message)
          else
            logger.debug(&generate_message)
          end
        rescue Exception => e
          logger.error { format_for_log(label, 'ERROR', statement) }
          raise
        end
        response
      end

      private

      def format_for_log(label, timing, statement)
        cql_for_log =
          case statement
          when String
            statement
          when Statement
            sanitize(statement.cql, limit_value_length(statement.bind_vars))
          when Cassandra::Statements::Batch
            batch_stmt = "BEGIN #{'UNLOGGED ' if statement.type == :unlogged}BATCH"
            statement.statements.each { |s| batch_stmt << "\n" << sanitize(s.cql, limit_value_length(s.params)) }
            batch_stmt << "END BATCH"
          end

        format('%s (%s) %s', label, timing, cql_for_log)
      end

      def limit_value_length(bind_vars)
        bind_vars.map { |it| String === it ? limit_length(it) : it }
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
