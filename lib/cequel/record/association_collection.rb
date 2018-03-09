# -*- encoding : utf-8 -*-
module Cequel
  module Record
    #
    # Collection of records from a
    # {Associations::ClassMethods#has_many has_many} association. Encapsulates
    # and behaves like a {RecordSet}, but unlike a normal RecordSet the loaded
    # records are held in memory after they are loaded.
    #
    # @see Associations::ClassMethods#has_many
    # @since 1.0.0
    #
    class AssociationCollection < DelegateClass(RecordSet)
      include Enumerable
      extend Util::Forwardable

      #
      # @yield [Record]
      # @return [void]
      #
      def each(&block)
        target.each(&block)
      end

      #
      # (see RecordSet#find)
      #
      def find(*keys)
        if block_given? then super
        else record_set.find(*keys)
        end
      end

      #
      # (see RecordSet#select)
      #
      def select(*columns)
        if block_given? then super
        else record_set.select(*columns)
        end
      end

      #
      # (see RecordSet#first)
      #
      def first(*args)
        if loaded? then super
        else record_set.first(*args)
        end
      end

      #
      # @raise [DangerousQueryError] to prevent loading the entire record set
      #   to be counted
      #
      def count
        raise Cequel::Record::DangerousQueryError.new
      end
      alias_method :length, :count
      alias_method :size, :count

      #
      # @return [Boolean] true if this collection's records are loaded in
      #   memory
      #
      def loaded?
        !!@target
      end

      private

      alias_method :record_set, :__getobj__

      def target
        @target ||= record_set.entries
      end
    end
  end
end
