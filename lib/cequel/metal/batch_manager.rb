# -*- encoding : utf-8 -*-
module Cequel
  module Metal
    #
    # Manage a current batch per thread. Used by {Keyspace}
    #
    # @api private
    #
    class BatchManager
      #
      # @param keyspace [Keyspace] keyspace to make writes to
      # @api private
      #
      def initialize(keyspace)
        @keyspace = keyspace
      end

      #
      # Execute write operations in a batch. Any inserts, updates, and deletes
      # inside this method's block will be executed inside a CQL BATCH
      # operation.
      #
      # @param options [Hash]
      # @option (see Batch#initialize)
      # @yield context within which all write operations will be batched
      # @return return value of block
      # @raise [ArgumentError] if attempting to start a logged batch while
      #   already in an unlogged batch, or vice versa.
      #
      # @example Perform inserts in a batch
      #   DB.batch do
      #     DB[:posts].insert(:id => 1, :title => 'One')
      #     DB[:posts].insert(:id => 2, :title => 'Two')
      #   end
      #
      # @note If this method is created while already in a batch of the same
      #   type (logged or unlogged), this method is a no-op.
      #
      def batch(options = {})
        new_batch = Batch.new(keyspace, options)

        if current_batch
          if current_batch.unlogged? && new_batch.logged?
            fail ArgumentError,
                 "Already in an unlogged batch; can't start a logged batch."
          end
          return yield(current_batch)
        end

        begin
          self.current_batch = new_batch
          yield(new_batch).tap { new_batch.apply }
        ensure
          self.current_batch = nil
        end
      end

      private

      attr_reader :keyspace

      def current_batch
        ::Thread.current[batch_key]
      end

      def current_batch=(batch)
        ::Thread.current[batch_key] = batch
      end

      def batch_key
        :"cequel-batch-#{object_id}"
      end
    end
  end
end
