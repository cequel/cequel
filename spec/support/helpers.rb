module Cequel

  module SpecSupport

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
    end

  end

end
