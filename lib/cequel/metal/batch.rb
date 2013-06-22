require 'stringio'

module Cequel

  module Metal

    #
    # Encapsulates a batch operation
    #
    # @see Keyspace::batch
    #
    class Batch

      #
      # @param keyspace [Keyspace] the keyspace that this batch will be executed on
      # @param options [Hash]
      # @option options (see Keyspace#batch)
      # @see Keyspace#batch
      # @todo support batch-level consistency options
      #
      def initialize(keyspace, options = {})
        @keyspace = keyspace
        @auto_apply = options[:auto_apply]
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
        @statement.append("APPLY BATCH\n")
        @keyspace.execute(*@statement.args)
      end

      private

      def reset
        @statement = Statement.new
        @statement.append("BEGIN BATCH\n")
        @statement_count = 0
      end

    end

  end

end
