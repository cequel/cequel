module Cequel

  module SpecSupport

    RowStub = Struct.new(:to_hash)

    class ResultStub

      def initialize(rows)
        @rows = rows
      end

      def fetch
        while row = fetch_row
          yield RowStub.new(row)
        end
      end

      def fetch_row
        @rows.shift
      end

    end

  end

end
