module Cequel

  module SpecSupport

    Connection = Object.new

    module Helpers

      def result_stub(*results)
        ResultStub.new(results)
      end

      def connection
        Connection
      end

      def cequel
        @cequel ||= Cequel::Keyspace.new(connection)
      end
    end

  end

end
