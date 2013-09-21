module Cequel

  module Record

    class AssociationCollection < DelegateClass(RecordSet)

      include Enumerable

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
