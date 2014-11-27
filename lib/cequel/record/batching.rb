module Cequel
  module Record
    #
    # Exposes batching-related methods on Record class singletons
    #
    # @see Metal::BatchManager
    #
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
