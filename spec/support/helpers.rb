module Cequel

  module SpecSupport
    module Macros
      def model(class_name, options = {}, &block)
        clazz = Class.new(Cequel::Base) do
          self.table_name = class_name.to_s.tableize
          class_eval(&block)
        end

        let(:model_class) { clazz }
        let(:mc) { clazz }

        if options.fetch(:create_table, true)
          before(:all) { clazz.synchronize_schema }
          after(:all) { cequel.schema.drop_table(clazz.table_name) }
          before :each do
            scope = cequel[clazz.table_name]
            keys = clazz.table_schema.key_columns.map(&:name)
            scope.each { |row| scope.where(row.slice(*keys)).delete }
          end
        end

        around do |example|
          Kernel.module_eval do
            if const_defined?(class_name)
              previous = const_get(class_name)
              remove_const(class_name)
            end
            const_set(class_name, clazz)
            example.run
            remove_const(class_name)
            const_set(class_name, previous) if previous
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
          :host => host,
          :keyspace => keyspace_name
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

      def cequel
        Helpers.cequel
      end

      def max_statements!(number)
        cequel.should_receive(:execute).at_most(number).times.and_call_original
      end

    end

  end

end
