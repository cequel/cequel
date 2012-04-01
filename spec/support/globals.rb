module Cequel
  module SpecSupport
    module Globals
      def connection
        @connection ||= stub('Connection')
      end

      def cequel
        @cequel ||= Cequel::Keyspace.new(connection)
      end
    end
  end
end
