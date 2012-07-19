module Cequel

  #
  # Handle to a Cassandra keyspace.
  #
  class Keyspace

    #
    # Set a logger for logging queries. Queries logged at INFO level
    #
    attr_writer :logger, :slowlog, :slowlog_threshold, :connection

    #
    # @api private
    # @see Cequel.connect
    #
    def initialize(configuration = {})
      @name = configuration[:keyspace]
      @hosts = configuration[:host] || configuration[:hosts]
      @thrift_options = configuration[:thrift].try(:symbolize_keys)
    end

    def connection
      @connection ||= CassandraCQL::Database.new(
        @hosts, {:keyspace => @name}, @thrift_options
      )
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
      log('CQL', statement, *bind_vars) do
        connection.execute(statement, *bind_vars)
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
        @batch.execute(statement, *bind_vars)
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

    def log(label, statement, *bind_vars)
      return yield unless @logger || @slowlog
      response = nil
      time = Benchmark.ms { response = yield }
      generate_message = proc do
        sprintf(
          '%s (%dms) %s', label, time.to_i,
          CassandraCQL::Statement.sanitize(statement, bind_vars)
        )
      end
      @logger.debug(&generate_message) if @logger
      threshold = @slowlog_threshold || 2000
      @slowlog.warn(&generate_message) if @slowlog && time >= threshold
      response
    end

  end

end
