module Cequel

  module SpecSupport

    module Helpers

      def result_stub(*results)
        ResultStub.new(results)
      end

      def connection
        @connection ||= stub('Connection')
      end

      def cequel
        @cequel ||= Cequel::Keyspace.new(connection)
      end
    end

  end

end
