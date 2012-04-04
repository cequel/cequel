require 'stringio'

module Cequel

  #
  # Encapsulates a batch operation
  #
  # @see Keyspace::batch
  #
  class Batch

    include Helpers

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
      @statements.puts(sanitize(cql, *bind_vars))
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
      @statements.puts("APPLY BATCH")
      @keyspace.execute(@statements.string)
    end

    private

    def reset
      @statements = StringIO.new
      @statements.puts("BEGIN BATCH")
      @statement_count = 0
    end

  end

end
