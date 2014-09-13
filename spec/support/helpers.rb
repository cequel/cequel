# -*- encoding : utf-8 -*-
module Cequel
  module SpecSupport
    module Macros
      def model(class_name, options = {}, &block)
        return if RSpec.configuration.filter_manager.exclude?(self)
        setup_models = !self.metadata.key?(:models)
        self.metadata[:models] ||= {}

        metadata[:models][class_name] = [options, block]

        if setup_models
          before :all do
            metadata = self.class.metadata
            metadata[:models].each do |name, (options, block)|
              clazz = Class.new do
                include Cequel::Record
                self.table_name = name.to_s.tableize + "_" + SecureRandom.hex(4)
                class_eval(&block)
              end
              Object.module_eval { const_set(name, clazz) }
            end
            metadata[:models].each_key do |name|
              if options.fetch(:synchronize_schema, true)
                Object.const_get(name).synchronize_schema
              end
            end
          end

          before :each do
            metadata = self.class.metadata
            metadata[:models].each_key do |name|
              name.to_s.constantize.find_each(&:destroy)
            end
          end

          after :all do
            self.class.metadata[:models].each_key do |name|
              cequel.schema.drop_table(Object.const_get(name).table_name)
              Object.module_eval { remove_const(name) }
            end
          end
        end
      end

      def uuid(name)
        let(name) { Cequel.uuid }
      end
    end

    module Helpers

      def self.cequel
        @cequel ||= Cequel.connect(
          host: host,
          port: port,
          keyspace: keyspace_name
        ).tap do |cequel|
          if ENV['CEQUEL_LOG_QUERIES']
            cequel.logger = Logger.new(STDOUT)
          else
            cequel.logger = Logger.new(File.open('/dev/null', 'a'))
          end
        end
      end

      def self.host
        '127.0.0.1'
      end

      def self.port
        ENV['CEQUEL_TEST_PORT'] || '9042'
      end

      def self.legacy_host
        ENV['CEQUEL_TEST_LEGACY_HOST'] || '127.0.0.1:9160'
      end

      def self.keyspace_name
        ENV['CEQUEL_TEST_KEYSPACE'] || 'cequel_test'
      end

      def self.legacy_connection
        require 'cassandra-cql'
        @legacy_connection ||= CassandraCQL::Database.new(
          legacy_host,
          :keyspace => keyspace_name,
          :cql_version => '2.0.0'
        )
      end

      def min_uuid(time = Time.now)
        Cql::TimeUuid::Generator.new(0, 0).from_time(time, 0)
      end

      def max_uuid(time = Time.now)
        Cql::TimeUuid::Generator.new(0x3fff, 0xffffffffffff).
          from_time(time, 999)
      end

      def cequel
        Helpers.cequel
      end

      def legacy_connection
        Helpers.legacy_connection
      end

      def max_statements!(number)
        cequel.client.should_receive(:execute).at_most(number).times.and_call_original
      end

      def disallow_queries!
        cequel.client.should_not_receive(:execute)
      end

      def with_client_error(error)
        cequel.client.stub(:execute).and_raise(error)
        begin
          yield
        ensure
          cequel.client.unstub(:execute)
        end
      end

      def expect_query_with_consistency(matcher, consistency)
        expect(cequel.client).to receive(:execute).with(matcher, consistency)
          .and_call_original
        yield
        RSpec::Mocks.proxy_for(cequel.client).reset
      end
    end
  end
end
