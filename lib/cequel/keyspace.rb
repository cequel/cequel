module Cequel

  #
  # Handle to a Cassandra keyspace.
  #
  class Keyspace

    #
    # @api private
    # @see Cequel.connect
    #
    def initialize(configuration={})
    end

    def self.connection=(connection)
      @connection = connection
    end

    def self.configure(configuration = {})
      @configuration = configuration
      @hosts = configuration[:host] || configuration[:hosts]
      @thrift_options = configuration[:thrift].try(:symbolize_keys) || {}
      @keyspace = configuration[:keyspace]
      # reset the connections
      @connection_pool = nil
    end

    def self.configuration
      @configuration
    end

    def self.logger=(logger)
      @logger = logger
    end

    def self.logger
      @logger
    end

    def self.slowlog=(slowlog)
      @slowlog = slowlog
    end

    def self.slowlog
      @slowlog
    end

    def self.slowlog_threshold=(slowlog_threshold)
      @slowlog_threshold = slowlog_threshold
    end

    def self.slowlog_threshold
      @slowlog_threshold
    end

    def self.connection_pool
      @connection_pool ||= ConnectionPool.new(:size => 10, :timeout => 5) do
        connection
      end
    end

    def self.connection
      options = @keyspace ? {:keyspace => @keyspace } : {}
      @connection || CassandraCQL::Database.new(
          @hosts,
          options,
          @thrift_options
        )
    end

    def self.clear_active_connections!
      @connection_pool = nil
    end

    def with_connection(&block)
      self.class.connection_pool.with do |conn|
        block.call(conn)
      end
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
        with_connection do |conn|
          conn.execute(statement, *bind_vars)
        end
      end
    end

    #
    # Write data to this keyspace using a CQL query. Will be included the
    # current batch operation if one is present.
    #
    # @param (see #execute)
    #
    def write(statement, *bind_vars)
      if get_batch
        get_batch.execute(statement, *bind_vars)
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
      old_batch = get_batch
      new_batch = Batch.new(self, options)
      set_batch(new_batch)
      yield
      new_batch.apply
    ensure
      set_batch(old_batch)
    end

    def self.create(configuration)
      configure(configuration.merge(:keyspace => nil))
      connection.execute "CREATE KEYSPACE #{configuration[:keyspace]} with #{configuration[:strategy]}"
      configure(configuration)
    end

    def self.drop(configuration)
      configure(configuration.merge(:keyspace => nil))
      connection.execute "DROP KEYSPACE #{configuration[:keyspace]}"
    end

  private

    def get_batch
      ::Thread.current[batch_key]
    end

    def set_batch(batch)
      ::Thread.current[batch_key] = batch
    end

    def batch_key
      :"cequel-batch-#{object_id}"
    end

    def log(label, statement, *bind_vars)
      return yield unless self.class.logger || self.class.slowlog
      response = nil
      time = Benchmark.ms { response = yield }
      generate_message = proc do
        sprintf(
          '%s (%dms) %s', label, time.to_i,
          CassandraCQL::Statement.sanitize(statement, bind_vars)
        )
      end
      self.class.logger.debug(&generate_message) if self.class.logger
      threshold = self.class.slowlog_threshold || 2000
      if self.class.slowlog && time >= threshold
        self.class.slowlog.warn(&generate_message)
      end
      response
    end

  end

end
