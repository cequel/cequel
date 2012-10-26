module Cequel

  module SpecSupport

    Connection = Object.new

    module Helpers

      def result_stub(*results)
        ResultStub.new(results)
      end

      def connection
        Connection.stub(:keyspace=)
        Connection
      end

      def cequel
        Cequel::Keyspace.connection = connection
        @cequel ||= Cequel::Keyspace.new({})
      end
    end

  end

end
