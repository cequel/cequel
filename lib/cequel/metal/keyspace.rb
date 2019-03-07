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
      extend Util::Forwardable
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
      # @return [Hash] SSL Configuration options
      attr_reader :ssl_config
      # @return [Symbol] The client compression option
      attr_reader :client_compression
      # @return [Hash] A hash of additional options passed to Cassandra, if any
      attr_reader :cassandra_options
      # @return [Object] The error policy object in use by this keyspace 
      attr_reader :error_policy

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

      # @!method write_with_options(statement, bind_vars, consistency)
      #
      #   Write data to this keyspace using a CQL query at the given
      #   consistency. Will be included the current batch operation if one is
      #   present.
      #
      #   @param (see #execute_with_options)
      #   @return [void]
      #
      def_delegator :write_target, :execute_with_options,
                    :write_with_options

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
        @lock = Monitor.new
        configure(configuration)
      end

      #
      # Configure this keyspace from a hash of options
      #
      # @param configuration [Options] configuration options
      # @option configuration [String] :host ('127.0.0.1') hostname of
      #   single Cassandra instance to connect to
      # @option configuration [Integer] :port (9042) port on which to connect
      #   to all specified hosts
      # @option configuration [Integer] :max_retries maximum number of retries
      #   on connection failure
      # @option configuration [Array<String>] :hosts list of Cassandra
      #   instances to connect to (hostnames only)
      # @option configuration [String] :username user to auth with (leave blank
      #   for no auth)
      # @option configuration [String] :password password to auth with (leave
      #   blank for no auth)
      # @option configuration [String] :keyspace name of keyspace to connect to
      # @option configuration [Boolean] :ssl enable/disable ssl/tls support
      # @option configuration [String] :server_cert path to ssl server
      #   certificate
      # @option configuration [String] :client_cert path to ssl client
      #   certificate
      # @option configuration [String] :private_key path to ssl client private
      #   key
      # @option configuration [String] :passphrase the passphrase for client
      #   private key
      # @option configuration [String] :cassandra_error_policy A mixin for 
      #   handling errors from Cassandra
      # @option configuration [Hash] :cassandra_options A hash of arbitrary
      #   options to pass to Cassandra
      # @return [void]
      #
      def configure(configuration = {})
        if configuration.key?(:thrift)
          warn "Cequel no longer uses the Thrift transport to communicate " \
               "with Cassandra. The :thrift option is deprecated and ignored."
        end
        @configuration = configuration
        
        @error_policy = extract_cassandra_error_policy(configuration)
        @cassandra_options = extract_cassandra_options(configuration)
        @hosts, @port = extract_hosts_and_port(configuration)
        @credentials  = extract_credentials(configuration)
        @ssl_config = extract_ssl_config(configuration)

        @name = configuration[:keyspace]
        @default_consistency = configuration[:default_consistency].try(:to_sym)
        @client_compression = configuration[:client_compression].try(:to_sym)

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
      # @return [Cassandra::Session] the low-level client session provided by the
      #   adapter
      # @api private
      #
      def client
        synchronize do
          @client ||= cluster.connect(name)
        end
      end

      #
      # Execute a CQL query in this keyspace
      #
      #   If a connection error occurs, will retry a maximum number of
      #   time (default 3) before re-raising the original connection
      #   error.
      #
      # @param statement [String] CQL string
      # @param bind_vars [Object] values for bind variables
      # @return [Enumerable] the results of the query
      #
      # @see #execute_with_options
      #
      def execute(statement, *bind_vars)
        execute_with_options(Statement.new(statement, bind_vars), { consistency: default_consistency })
      end

      #
      # Execute a CQL query in this keyspace with the given options
      #
      # @param statement [String,Statement,Batch] statement to execute
      # @param options [Options] options for statement execution
      # @return [Enumerable] the results of the query
      #
      # @since 1.1.0
      #
      def execute_with_options(statement, options={})
        options[:consistency] ||= default_consistency

        cql, options = *case statement
                        when Statement
                          [prepare_statement(statement),
                           {arguments: statement.bind_vars}.merge(options)]
                        when Cassandra::Statements::Batch
                          [statement, options]
                        end

        log('CQL', statement) do
          error_policy.execute_stmt(self) do
            client.execute(cql, options)
          end
        end
      end

      #
      # Wraps the prepare statement in the default retry strategy
      #
      # @param statement [String,Statement] statement to prepare
      # @return [Cassandra::Statement::Prepared] the prepared statement
      #
      def prepare_statement(statement)
        cql = case statement
              when Statement
                statement.cql
              else
                statement
              end
        error_policy.execute_stmt(self) do
          client.prepare(cql)
        end
      end

      #
      # Clears all active connections
      #
      # @return [void]
      #
      def clear_active_connections!
        synchronize do
          if defined? @client
            remove_instance_variable(:@client)
          end
          if defined? @client_without_keyspace
            remove_instance_variable(:@client_without_keyspace)
          end
          if defined? @cluster
            @cluster.close
            remove_instance_variable(:@cluster)
          end
        end
      end

      #
      # @return [Symbol] the default consistency for queries in this keyspace
      # @since 1.1.0
      #
      def default_consistency
        @default_consistency || :quorum
      end

      # @return [Boolean] true if the keyspace exists
      def exists?
        cluster.has_keyspace?(name)
      end

      # @return [String] Cassandra version number
      def cassandra_version
        return @cassandra_version if @cassandra_version

        statement = <<-CQL
          SELECT release_version
          FROM system.local
        CQL

        log('CQL', statement) do
          @cassandra_version = client_without_keyspace.execute(statement).first['release_version']
        end
      end

      # return true if Cassandra server version is known to include bug CASSANDRA-8733
      def bug8733_version?
        version_file = File.expand_path('../../../../.cassandra-versions', __FILE__)
        @all_versions ||= File.read(version_file).split("\n").map(&:strip)

        # bug exists in versions 0.3.0-2.0.12 and 2.1.0-2.1.2
        @bug8733_versions ||= @all_versions[0..@all_versions.index('2.0.12')] +
            @all_versions[@all_versions.index('2.1.0')..@all_versions.index('2.1.2')]

        @bug8733_versions.include?(cassandra_version)
      end

      def cluster
        synchronize do
          @cluster ||= Cassandra.cluster(client_options)
        end
      end

      private

      attr_reader :lock

      def_delegator :batch_manager, :current_batch
      private :current_batch

      def_delegator :lock, :synchronize
      private :lock

      def client_without_keyspace
        synchronize do
          @client_without_keyspace ||= cluster.connect
        end
      end

      def client_options
        {hosts: hosts, port: port}.tap do |options|
          options.merge!(credentials) if credentials
          options.merge!(ssl_config) if ssl_config
          options.merge!(compression: client_compression) if client_compression
          options.merge!(cassandra_options) if cassandra_options
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
        ports << Integer(configuration[:port]) if configuration.key?(:port)
        host_or_hosts =
          configuration.fetch(:host, configuration.fetch(:hosts, '127.0.0.1'))
        Array.wrap(host_or_hosts).each do |host_port|
          host, port = host_port.split(':')
          hosts << host
          if port
            warn "Specifying a hostname as host:port is deprecated. Specify " \
                 "only the host IP or hostname in :hosts, and specify a " \
                 "port for all nodes using the :port option."
            ports << Integer(port)
          end
        end

        if ports.size > 1
          fail ArgumentError, "All Cassandra nodes must listen on the same " \
               "port; specified multiple ports #{ports.join(', ')}"
        end

        [hosts, ports.first || 9042]
      end

      def extract_credentials(configuration)
        configuration.slice(:username, :password).presence
      end

      def extract_ssl_config(configuration)
        ssl_config = {}
        ssl_config[:ssl] = configuration.fetch(:ssl, nil)
        ssl_config[:server_cert] = configuration.fetch(:server_cert, nil)
        ssl_config[:client_cert] = configuration.fetch(:client_cert, nil)
        ssl_config[:private_key] = configuration.fetch(:private_key, nil)
        ssl_config[:passphrase] = configuration.fetch(:passphrase, nil)
        ssl_config.each { |key, value| ssl_config.delete(key) unless value }
        ssl_config
      end
      
      def extract_cassandra_error_policy(configuration)
        value = configuration.fetch(:cassandra_error_policy, ::Cequel::Metal::Policy::CassandraError::ClearAndRetryPolicy)
        # Accept a class name as a string, create an instance of it 
        if value.is_a?(String)
          value.constantize.new(configuration)
        # Accept a class, instantiate it
        elsif value.is_a?(Class)
          value.new(configuration)
        # Accept a value, assume it is a ready to use policy object
        else 
          value
        end
      end
      
      def extract_cassandra_options(configuration)
        configuration[:cassandra_options]
      end
    end
  end
end
