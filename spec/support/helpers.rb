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
        ENV['CEQUEL_TEST_HOST'] || '127.0.0.1'
      end

      def self.port
        ENV['CEQUEL_TEST_PORT'] || '9042'
      end

      def self.legacy_host
        ENV['CEQUEL_TEST_LEGACY_HOST'] || '127.0.0.1:9160'
      end

      def self.keyspace_name
        ENV.fetch('CEQUEL_TEST_KEYSPACE') do
          test_env_number = ENV['TEST_ENV_NUMBER']
          if test_env_number.present?
            "cequel_test_#{test_env_number}"
          else
            'cequel_test'
          end
        end
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

      def expect_statement_count(number)
        allow(cequel.client).to receive(:execute).and_call_original
        yield
        expect(cequel.client).to have_received(:execute).exactly(number).times
      end

      def disallow_queries!
        expect(cequel.client).to_not receive(:execute)
      end

      def with_client_error(error)
        allow(cequel.client).to receive(:execute).once.and_raise(error)
        begin
          yield
        ensure
          allow(cequel.client).to receive(:execute).and_call_original
        end
      end

      def expect_query_with_consistency(matcher, consistency)
        allow(cequel.client).to receive(:execute).and_call_original
        yield
        expect(cequel.client).to have_received(:execute).with(matcher, consistency)
      end
    end
  end
end
