require 'stringio'

module Cequel
  module Metal
    #
    # Encapsulates a batch operation
    #
    # @see Keyspace::batch
    # @api private
    #
    class Batch
      #
      # @param keyspace [Keyspace] the keyspace that this batch will be executed on
      # @param options [Hash]
      # @option options [Integer] :auto_apply If specified, flush the batch
      #   after this many statements have been added.
      # @option options [Boolean] :unlogged (false) Whether to use an [unlogged
      #   batch](http://www.datastax.com/documentation/cql/3.0/webhelp/cql/cql_reference/batch_r.html).
      #   Logged batches guarantee atomicity (but not isolation) at the
      #   cost of a performance penalty; unlogged batches are useful for bulk
      #   write operations but behave the same as discrete writes.
      # @see Keyspace#batch
      #
      def initialize(keyspace, options = {})
        @keyspace = keyspace
        @auto_apply = options[:auto_apply]
        @unlogged = options.fetch(:unlogged, false)
        reset
      end

      #
      # Add a statement to the batch.
      #
      # @param (see Keyspace#execute)
      #
      def execute(cql, *bind_vars)
        @statement.append("#{cql}\n", *bind_vars)
        @statement_count += 1
        if @auto_apply && @statement_count >= @auto_apply
          apply
          reset
        end
      end

      #
      # Send the batch to Cassandra
      #
      def apply
        return if @statement_count.zero?
        if @statement_count > 1
          @statement.prepend(begin_statement)
          @statement.append("APPLY BATCH\n")
        end
        @keyspace.execute(*@statement.args)
      end

      #
      # Is this an unlogged batch?
      #
      # @return [Boolean]
      def unlogged?
        @unlogged
      end

      #
      # Is this a logged batch?
      #
      # @return [Boolean]
      #
      def logged?
        !unlogged?
      end

      private

      def reset
        @statement = Statement.new
        @statement_count = 0
      end

      def begin_statement
        "BEGIN #{"UNLOGGED " if unlogged?}BATCH\n"
      end
    end
  end
end
