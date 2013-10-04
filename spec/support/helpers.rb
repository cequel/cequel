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
                self.table_name = name.to_s.tableize
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
        let(name) { CassandraCQL::UUID.new }
      end
    end

    module Helpers

      def self.cequel
        @cequel ||= Cequel.connect(
          host: host,
          keyspace: keyspace_name,
          thrift: {retries: 5, cached_connections: true}
        ).tap do |cequel|
          cequel.logger = Logger.new(STDOUT) if ENV['CEQUEL_LOG_QUERIES']
        end
      end

      def self.host
        ENV['CEQUEL_TEST_HOST'] || '127.0.0.1:9160'
      end

      def self.keyspace_name
        ENV['CEQUEL_TEST_KEYSPACE'] || 'cequel_test'
      end

      def self.legacy_connection
        @legacy_connection ||= CassandraCQL::Database.new(
          Cequel::SpecSupport::Helpers.host,
          :keyspace => Cequel::SpecSupport::Helpers.keyspace_name,
          :cql_version => '2.0.0'
        )
      end

      def min_uuid(time = Time.now)
        CassandraCQL::UUID.new(time, :randomize => false)
      end

      def max_uuid(time = Time.now)
        time = time.stamp * 10 + SimpleUUID::UUID::GREGORIAN_EPOCH_OFFSET
        # See http://github.com/spectra/ruby-uuid/
        byte_array = [
          time & 0xFFFF_FFFF,
          time >> 32,
          ((time >> 48) & 0x0FFF) | 0x1000,
          (2**13 - 1) | SimpleUUID::UUID::VARIANT,
          2**16 - 1,
          2**32 - 1
        ]
        CassandraCQL::UUID.new(byte_array.pack("NnnnnN"))
      end

      def cequel
        Helpers.cequel
      end

      def legacy_connection
        Helpers.legacy_connection
      end

      def max_statements!(number)
        cequel.should_receive(:execute).at_most(number).times.and_call_original
      end

      def disallow_queries!
        cequel.should_not_receive(:execute)
      end

    end

  end

end
