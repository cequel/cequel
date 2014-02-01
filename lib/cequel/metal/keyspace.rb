module Cequel
  module Metal
    #
    # Handle to a Cassandra keyspace (database). Keyspace objects are factories
    # for DataSet instances and provide a handle to a Schema::Keyspace
    # instance.
    #
    class Keyspace
      extend Forwardable
      include Logging

      # @return [Hash] configuration options for this keyspace
      attr_reader :configuration
      # @return [String] name of the keyspace
      attr_reader :name

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
      # @!method batch
      #   (see Cequel::Metal::BatchManager#batch)
      #
      def_delegator :batch_manager, :batch

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
      # @option configuration [Array<String>] :hosts list of Cassandra
      #   instances to connect to
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
        @hosts = configuration.fetch(
          :host, configuration.fetch(:hosts, '127.0.0.1:9160'))
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
      # Clears all active connections
      #
      # @return [void]
      #
      def clear_active_connections!
        if defined? @connection_pool
          remove_instance_variable(:@connection_pool)
        end
      end

      private

      def_delegator :connection_pool, :with, :with_connection
      private :with_connection

      def_delegator :batch_manager, :current_batch
      private :current_batch

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

      def batch_manager
        @batch_manager ||= BatchManager.new(self)
      end

      def write_target
        current_batch || self
      end
    end
  end
end
