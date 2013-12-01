module Cequel
  module Record
    #
    # Collection of records from a {Associations::ClassMethods#has_many has_many}
    # associaiton. Encapsulates and behaves like a {RecordSet}, but unlike a
    # normal RecordSet the loaded records are held in memory after they are
    # loaded.
    #
    # @since 1.0.0
    #
    class AssociationCollection < DelegateClass(RecordSet)
      include Enumerable

      #
      # @yield [Record]
      # @return [void]
      #
      def each(&block)
        target.each(&block)
      end

      private

      def target
        @target ||= __getobj__.entries
      end
    end
  end
end
