# -*- encoding : utf-8 -*-
require 'set'

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
      include MonitorMixin

      # @return [Hash] configuration options for this keyspace
      attr_reader :configuration
      # @return [String] name of the keyspace
      attr_reader :name
      # @return [Array<String>] list of hosts to connect to
      attr_reader :hosts
      # @return Integer port to connect to Cassandra nodes on
      attr_reader :port
      # @return [Symbol] the default consistency for queries in this keyspace
      # @since 1.1.0
      attr_writer :default_consistency
      # @return [Hash] credentials for connect to cassandra
      attr_reader :credentials

      #
      # @!method write(statement, *bind_vars)
      #
      #   Write data to this keyspace using a CQL query. Will be included the
      #   current batch operation if one is present.
      #
      #   @param (see #execute)
      #   @return [void]
      #
      def_delegator :write_target, :execute, :write

      # @!method write_with_consistency(statement, bind_vars, consistency)
      #
      #   Write data to this keyspace using a CQL query at the given
      #   consistency. Will be included the current batch operation if one is
      #   present.
      #
      #   @param (see #execute_with_consistency)
      #   @return [void]
      #
      def_delegator :write_target, :execute_with_consistency,
                    :write_with_consistency

      #
      # @!method batch
      #   (see Cequel::Metal::BatchManager#batch)
      #
      def_delegator :batch_manager, :batch

      #
      # Combine a statement with bind vars into a fully-fledged CQL query. This
      # will no longer be needed once the CQL driver supports bound values
      # natively.
      #
      # @param statement [String] CQL statement with ? placeholders for bind
      #   vars
      # @param bind_vars [Array] bind variables corresponding to ? in the
      #   statement
      # @return [String] CQL statement with quoted values in place of bind
      #   variables
      #
      def self.sanitize(statement, bind_vars)
        each_bind_var = bind_vars.each
        statement.gsub('?') { Type.quote(each_bind_var.next) }
      end

      #
      # @!method sanitize
      #   (see Cequel::Metal::Keyspace.sanitize)
      #
      def_delegator 'self.class', :sanitize

      #
      # @api private
      # @param configuration [Options]
      # @option (see #configure)
      # @see Cequel.connect
      #
      def initialize(configuration={})
        configure(configuration)
        @lock = Monitor.new
      end

      #
      # Configure this keyspace from a hash of options
      #
      # @param configuration [Options] configuration options
      # @option configuration [String] :host ('127.0.0.1:9042') host/port of
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
        if configuration.key?(:thrift)
          warn "Cequel no longer uses the Thrift transport to communicate " \
               "with Cassandra. The :thrift option is deprecated and ignored."
        end
        @configuration = configuration

        @hosts, @port = extract_hosts_and_port(configuration)
        @credentials = extract_credentials(configuration)

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
      # @return [Cql::Client::Client] the low-level client provided by the
      #   adapter
      # @api private
      #
      def client
        synchronize { @client ||= build_client }
      end

      #
      # Execute a CQL query in this keyspace
      #
      # @param statement [String] CQL string
      # @param bind_vars [Object] values for bind variables
      # @return [Enumerable] the results of the query
      #
      # @see #execute_with_consistency
      #
      def execute(statement, *bind_vars)
        execute_with_consistency(statement, bind_vars, default_consistency)
      end

      #
      # Execute a CQL query in this keyspace with the given consistency
      #
      # @param statement [String] CQL string
      # @param bind_vars [Array] array of values for bind variables
      # @param consistency [Symbol] consistency at which to execute query
      # @return [Enumerable] the results of the query
      #
      # @since 1.1.0
      #
      def execute_with_consistency(statement, bind_vars, consistency)
        log('CQL', statement, *bind_vars) do
          client.execute(sanitize(statement, bind_vars),
                         consistency || default_consistency)
        end
      end

      #
      # Clears all active connections
      #
      # @return [void]
      #
      def clear_active_connections!
        if defined? @client
          remove_instance_variable(:@client)
        end
      end

      #
      # @return [Symbol] the default consistency for queries in this keyspace
      # @since 1.1.0
      #
      def default_consistency
        @default_consistency || :quorum
      end

      private

      attr_reader :lock

      def_delegator :batch_manager, :current_batch
      private :current_batch

      def_delegator :lock, :synchronize
      private :lock

      def build_client
        Cql::Client.connect(hosts: hosts, port: port, credentials: credentials).tap do |client|
          client.use(name) if name
        end
      end

      def batch_manager
        synchronize { @batch_manager ||= BatchManager.new(self) }
      end

      def write_target
        current_batch || self
      end

      def extract_hosts_and_port(configuration)
        hosts, ports = [], Set[]
        ports << configuration[:port] if configuration.key?(:port)
        Array.wrap(configuration.fetch(
          :host, configuration.fetch(:hosts, '127.0.0.1'))).each do |host_port|

          host, port = host_port.split(':')
          hosts << host
          if port
            warn "Specifying a hostname as host:port is deprecated. Specify " \
                 "only the host IP or hostname in :hosts, and specify a " \
                 "port for all nodes using the :port option."
            ports << port.to_i
          end
        end

        if ports.size > 1
          fail ArgumentError, "All Cassandra nodes must listen on the same " \
               "port; specified multiple ports #{ports.join(', ')}"
        end

        [hosts, ports.first || 9042]
      end

      def extract_credentials(configuration)
        _credentials = { username: '', password: '' }
        _credentials[:username] = configuration[:username] if configuration.key?(:username)
        _credentials[:password] = configuration[:password] if configuration.key?(:password)

        _credentials
      end
    end
  end
end
