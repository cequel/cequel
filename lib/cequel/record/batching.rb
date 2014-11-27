module Cequel
  module Record
    module Batching
      extend Forwardable

      #
      # @!method batch
      #   (see Cequel::Metal::Keyspace#batch)
      #
      def_delegator :connection, :batch

      #
      # @!method in_batch?
      #   (see Cequel::Metal::Keyspace#batch)
      #
      def_delegator :connection, :in_batch?
    end
  end
end
