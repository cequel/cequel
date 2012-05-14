module Cequel

  #
  # Handle to a Cassandra keyspace.
  #
  class Keyspace

    include Helpers

    #
    # Set a logger for logging queries. Queries logged at INFO level
    #
    attr_writer :logger, :slowlog, :slowlog_threshold

    #
    # @api private
    # @see Cequel.connect
    #
    def initialize(connection)
      @connection = connection
    end

    #
    # Get DataSet encapsulating a column family in this keyspace
    #
    # @param column_family_name [Symbol] the name of the column family
    # @return [DataSet] a column family
    #
    def [](column_family_name)
      DataSet.new(column_family_name.to_sym, self)
    end

    #
    # Execute a CQL query in this keyspace.
    #
    # @param statement [String] CQL string
    # @param *bind_vars [Object] values for bind variables
    #
    def execute(statement, *bind_vars)
      log('CQL', statement) do
        @connection.execute(statement, *bind_vars)
      end
    end

    #
    # Write data to this keyspace using a CQL query. Will be included the
    # current batch operation if one is present.
    #
    # @param (see #execute)
    #
    def write(statement, *bind_vars)
      if @batch
        @batch.execute(sanitize(statement, *bind_vars))
      else
        execute(statement, *bind_vars)
      end
    end

    #
    # Execute write operations in a batch. Any inserts, updates, and deletes
    # inside this method's block will be executed inside a CQL BATCH operation.
    #
    # @param options [Hash]
    # @option options [Fixnum] :auto_apply Automatically send batch to Cassandra after this many statements
    #
    # @example Perform inserts in a batch
    #   DB.batch do
    #     DB[:posts].insert(:id => 1, :title => 'One')
    #     DB[:posts].insert(:id => 2, :title => 'Two')
    #   end
    #
    def batch(options = {})
      old_batch, @batch = @batch, Batch.new(self, options)
      yield
      @batch.apply
    ensure
      @batch = old_batch
    end

    private

    def log(label, message)
      return yield unless @logger || @slowlog
      response = nil
      time = Benchmark.ms do
        response = yield
      end
      if @logger
        @logger.info { sprintf('%s (%dms) %s', label, time.to_i, message) }
      end
      threshold = @slowlog_threshold || 2
      if @slowlog && time >= threshold
        @slowlog.warn { sprintf('%s (%dms) %s', label, time.to_i, message) }
      end
      response
    end

  end

end
