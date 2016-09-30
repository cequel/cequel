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
      def execute(statement)
        @statements << statement
        if @auto_apply && @statements.size >= @auto_apply
          apply
          reset
        end
      end

      #
      # Send the batch to Cassandra
      #
      def apply
        return if @statements.empty?

        statement = @statements.first
        if @statements.size > 1
          statement =
            if logged?
              keyspace.client.logged_batch
            else
              keyspace.client.unlogged_batch
            end
          @statements.each { |s| statement.add(s.prepare(keyspace), arguments: s.bind_vars) }
        end

        keyspace.execute_with_options(statement, consistency: @consistency)
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
      def execute_with_options(statement, options)
        query_consistency = options.fetch(:consistency)
        if query_consistency && query_consistency != @consistency
          raise ArgumentError,
                "Attempting to perform query with consistency " \
                "#{query_consistency.to_s.upcase} in batch with consistency " \
                "#{@consistency.upcase}"
        end
        execute(statement)
      end

      private

      attr_reader :on_complete_hooks, :keyspace

      def reset
        @statements = []
        @statement_count = 0
        @on_complete_hooks = []
      end

      def execute_on_complete_hooks
        on_complete_hooks.each { |hook| hook.call }
      end
    end
  end
end
