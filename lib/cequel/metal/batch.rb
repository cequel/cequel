# -*- encoding : utf-8 -*-
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
      # @param keyspace [Keyspace] the keyspace that this batch will be
      #   executed on
      # @param options [Hash]
      # @option options [Integer] :auto_apply If specified, flush the batch
      #   after this many statements have been added.
      # @option options [Boolean] :unlogged (false) Whether to use an [unlogged
      #   batch](
      #   http://www.datastax.com/dev/blog/atomic-batches-in-cassandra-1-2).
      #   Logged batches guarantee atomicity (but not isolation) at the
      #   cost of a performance penalty; unlogged batches are useful for bulk
      #   write operations but behave the same as discrete writes.
      # @see Keyspace#batch
      #
      def initialize(keyspace, options = {})
        options.assert_valid_keys(:auto_apply, :unlogged, :consistency)
        @keyspace = keyspace
        @auto_apply = options[:auto_apply]
        @unlogged = options.fetch(:unlogged, false)
        @consistency = options.fetch(:consistency,
                                     keyspace.default_consistency)
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
        @keyspace.execute_with_consistency(
          @statement.args.first, @statement.args.drop(1), @consistency)
        execute_on_complete_hooks
      end

      def on_complete(&block)
        on_complete_hooks << block
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

      # @private
      def execute_with_consistency(cql, bind_vars, query_consistency)
        if query_consistency && query_consistency != @consistency
          raise ArgumentError,
                "Attempting to perform query with consistency " \
                "#{query_consistency.to_s.upcase} in batch with consistency " \
                "#{@consistency.upcase}"
        end
        execute(cql, *bind_vars)
      end

      private

      attr_reader :on_complete_hooks

      def reset
        @statement = Statement.new
        @statement_count = 0
        @on_complete_hooks = []
      end

      def begin_statement
        "BEGIN #{"UNLOGGED " if unlogged?}BATCH\n"
      end

      def execute_on_complete_hooks
        on_complete_hooks.each { |hook| hook.call }
      end
    end
  end
end
