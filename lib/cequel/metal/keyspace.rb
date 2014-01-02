module Cequel
  module Metal
    #
    # Handle to a Cassandra keyspace (database). Keyspace objects are factories
    # for DataSet instances and provide a handle to a Schema::Keyspace
    # instance.
    #
    class Keyspace
      extend Forwardable

      # @return [Hash] configuration options for this keyspace
      attr_reader :configuration
      # @return [String] name of the keyspace
      attr_reader :name
      # @return [Logger] logger to be used for CQL statements
      attr_accessor :logger
      # @return [Logger] logger to be used for slow CQL statements
      attr_accessor :slowlog
      # @return [Integer] threshold in ms for statements to appear in the
      #   slowlog
      attr_writer :slowlog_threshold

      #
      # @!method write(statement, *bind_vars)
      #
      # Write data to this keyspace using a CQL query. Will be included the
      # current batch operation if one is present.
      #
      # @param (see #execute)
      # @return [void]
      #
      def_delegator :write_target, :execute, :write

      #
      # @api private
      # @param configuration [Options]
      # @option (see #configure)
      # @see Cequel.connect
      #
      def initialize(configuration={})
        configure(configuration)
      end

      #
      # Configure this keyspace from a hash of options
      #
      # @param configuration [Options] configuration options
      # @option configuration [String] :host ('127.0.0.1:9160') host/port of
      #   single Cassandra instance to connect to
      # @option configuration [Array<String>] :hosts list of Cassandra instances
      #   to connect to
      # @option configuration [Hash] :thrift Thrift options to be passed
      #   directly to Thrift client
      # @option configuration [String] :keyspace name of keyspace to connect to
      # @option configuration [Integer] :pool (1) size of connection pool
      # @option configuration [Integer] :pool_timeout (0) timeout when
      #   attempting to check out connection from pool
      # @return [void]
      #
      def configure(configuration = {})
        @configuration = configuration
        @hosts = configuration.fetch(:host, configuration.fetch(:hosts, '127.0.0.1:9160'))
        @thrift_options = configuration[:thrift].try(:symbolize_keys) || {}
        @name = configuration[:keyspace]
        # reset the connections
        clear_active_connections!
      end

      #
      # @return [Schema::Keyspace] schema object providing full read/write
      #   access to database schema
      def schema
        Schema::Keyspace.new(self)
      end

      #
      # @param table_name [Symbol] the name of the table
      # @return [DataSet] data set encapsulating table
      #
      def [](table_name)
        DataSet.new(table_name.to_sym, self)
      end

      #
      # Execute a CQL query in this keyspace
      #
      # @param statement [String] CQL string
      # @param bind_vars [Object] values for bind variables
      # @return [void]
      #
      def execute(statement, *bind_vars)
        log('CQL', statement, *bind_vars) do
          with_connection do |conn|
            conn.execute(statement, *bind_vars)
          end
        end
      end

      #
      # Execute write operations in a batch. Any inserts, updates, and deletes
      # inside this method's block will be executed inside a CQL BATCH operation.
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
        new_batch = Batch.new(self, options)

        if current_batch
          if current_batch.unlogged? && new_batch.logged?
            fail ArgumentError,
                 "Already in an unlogged batch; can't start a logged batch."
          end
          return yield
        end

        begin
          self.current_batch = new_batch
          yield.tap { new_batch.apply }
        ensure
          self.current_batch = nil
        end
      end

      #
      # Clears all active connections, either single connection or connection pool
      #
      # @return [void]
      #
      def clear_active_connections!
        remove_instance_variable(:@connection_pool) if defined? @connection_pool
      end

      private

      def_delegator :connection_pool, :with, :with_connection
      private :with_connection

      def build_connection
        options = {cql_version: '3.0.0'}
        options[:keyspace] = name if name
        CassandraCQL::Database.new(
          @hosts,
          options,
          @thrift_options
        )
      end

      def connection_pool
        return @connection_pool if defined? @connection_pool
        options = {
          size: @configuration.fetch(:pool, 1),
          timeout: @configuration.fetch(:pool_timeout, 0)
        }
        @connection_pool = ConnectionPool.new(options) do
          build_connection
        end
      end

      def write_target
        current_batch || self
      end

      def current_batch
        ::Thread.current[batch_key]
      end

      def current_batch=(batch)
        ::Thread.current[batch_key] = batch
      end

      def batch_key
        :"cequel-batch-#{object_id}"
      end

      def log(label, statement, *bind_vars)
        response = nil
        begin
          time = Benchmark.ms { response = yield }
        rescue Exception => e
          log_statement(logger: logger, severity: :error, label: label,
                        statement: statement, bind_vars: bind_vars)
          raise
        end
        log_statement(logger: logger, severity: :debug, label: label,
                      statement: statement, bind_vars: bind_vars, 
                      timing: time.to_i)
        if time >= slowlog_threshold
          log_statement(logger: slowlog, severity: :warn,
                        label: label, statement: statement,
                        bind_vars: bind_vars, timing: time.to_i)
        end
        response
      end

      def slowlog_threshold
        @slowlog_threshold || 2000
      end

      private

      def log_statement(args)
        logger, severity, label, statement, bind_vars =
          args.fetch(:logger), args.fetch(:severity),
          args.fetch(:label), args.fetch(:statement), args.fetch(:bind_vars)
        timing = args[:timing]

        if logger
          logger.add(severity) do
            pattern = timing ? '%s (%dms) %s' : '%s (ERROR) %s'
            sprintf(
              pattern, label, timing,
              CassandraCQL::Statement.sanitize(statement, bind_vars)
            )
          end
        end
      end
    end
  end
end
