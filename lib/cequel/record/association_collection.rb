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
      # @!method count
      #   Get the count of child records stored in the database. This method
      #   will always query Cassandra, even if the records are loaded in
      #   memory.
      #
      #   @return [Integer] number of child records in the database
      #   @see #size
      #   @see #length
      #
      def_delegator :record_set, :count

      #
      # @!method length
      #   The number of child instances in the in-memory collection. If the
      #   records are not loaded in memory, they will be loaded and then
      #   counted.
      #
      #   @return [Integer] length of the loaded record collection in memory
      #   @see #size
      #   @see #count
      #
      def_delegator :entries, :length

      #
      # Get the size of the child collection. If the records are loaded in
      # memory from a previous operation, count the length of the array in
      # memory. If the collection is unloaded, perform a `COUNT` query.
      #
      # @return [Integer] size of the child collection
      # @see #length
      # @see #count
      #
      def size
        loaded? ? length : count
      end

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
