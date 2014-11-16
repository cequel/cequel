# -*- encoding : utf-8 -*-
require 'delegate'

module Cequel
  module Record
    #
    # The value of a collection column in a {Record}. Collections track
    # modifications that can be expressed as atomic collection mutations in
    # CQL, and persist those modifications when their owning record is saved.
    # Such modifications can be done even if the collection has not loaded
    # data from CQL, in the case of an unloaded record or where the collection
    # column was not included in the `SELECT` statement.
    #
    # Mutation operations that require reading data before writing it are not
    # supported (e.g. `Array#map!).
    #
    # Each collection implementation wraps a built-in Ruby collection type.
    #
    # @abstract Including classes must descend from `Delegator` and implement
    #   the `::empty` class method.
    #
    # @example
    #   class Blog
    #     include Cequel::Record
    #
    #     key :subdomain
    #
    #     list :categories, :text
    #   end
    #
    #   # Get an unloaded Blog instance; no data read
    #   blog = Blog['cassandra']
    #
    #   # Stage modification to collection, still no data read
    #   blog.categories << 'Big Data'
    #
    #   # Issue an UPDATE statement which pushes "Big Data" onto the
    #   # collection. Still no data read
    #   blog.save!
    #
    #   # Stage another modification to the collection
    #   blog.categories.unshift('Distributed Database')
    #
    #   # Collection is lazily read from the database, and then staged
    #   # modifications are made to the loaded collection
    #   puts blog.categories.join(', ') 
    #
    #   # Issues an UPDATE statement which prepends "Distributed Data" onto the
    #   # collection
    #   blog.save! 
    #
    # @since 1.0.0
    #
    module Collection
      extend ActiveSupport::Concern
      extend Forwardable

      #
      # @!method loaded?
      #   @return [Boolean] `true` if the collection's contents are loaded into
      #     memory
      #
      def_delegators :@model, :loaded?, :updater, :deleter
      private :updater, :deleter

      #
      # @!method column_name
      #   @return [Symbol] the name of the collection column
      #
      def_delegator :@column, :name, :column_name

      def_delegators :__getobj__, :clone, :dup

      included do
        define_method(
          :method_missing,
          BasicObject.instance_method(:method_missing))
        private :method_missing
      end

      #
      # @param model [Record] record that contains this collection
      # @param column [Schema::Column] column this collection's data belongs to
      # @return [Collection] a new collection
      #
      def initialize(model, column)
        @model, @column = model, column
      end

      #
      # @return [String] inspected underlying Ruby collection object
      #
      def inspect
        __getobj__.inspect
      end

      #
      # Notify the collection that its underlying data is loaded in memory.
      #
      # @return [void]
      #
      # @api private
      #
      def loaded!
        modifications.each { |modification| modification.call() }.clear
      end

      #
      # Notify the collection that its staged changes have been written to the
      # data store.
      #
      # @return [void]
      #
      # @api private
      #
      def persisted!
        modifications.clear
      end

      protected

      def __getobj__
        model.__send__(:read_attribute, column_name)
      end

      def __setobj__(obj)
        fail "Attempted to call __setobj__ on read-only delegate!"
      end

      private

      attr_reader :model, :column
      def_delegator :column, :cast, :cast_collection
      def_delegator 'column.type', :cast, :cast_element
      private :cast_collection, :cast_element

      def to_modify(&block)
        if loaded?
          model.__send__("#{column_name}_will_change!")
          block.call
        else modifications << block
        end
        self
      end

      def to_update
        yield unless model.new_record?
      end

      def modifications
        @modifications ||= []
      end
    end

    #
    # The value of a list column in a {Record} instance. List collections
    # encapsulate and behave like the built-in `Array` type.
    #
    # @see http://cassandra.apache.org/doc/cql3/CQL.html#list
    #   CQL documentation for the list type
    # @since 1.0.0
    #
    class List < DelegateClass(Array)
      include Collection

      # These methods are not available on lists because they require reading
      # collection data before writing it.
      NON_ATOMIC_MUTATORS = [
        :collect!,
        :delete_if,
        :fill,
        :flatten!,
        :insert,
        :keep_if,
        :map!,
        :pop,
        :reject!,
        :reverse!,
        :rotate!,
        :select!,
        :shift,
        :shuffle!,
        :slice!,
        :sort!,
        :sort_by!,
        :uniq!
      ]
      NON_ATOMIC_MUTATORS
        .each { |method| undef_method(method) if method_defined? method }

      #
      # Set the value at a position or range of positions. This modification
      # will be staged and persisted as an atomic list update when the record
      # is saved. If the collection data is loaded in memory, it will also be
      # modified accordingly.
      #
      # @return [void]
      #
      # @see DataSet#list_replace
      # @note Negative positions are not supported, as they are not allowed in
      #   CQL list operations.
      #
      # @overload []=(position, element)
      #
      #   @param position [Integer] position at which to set element
      #   @param element element to insert at position in list
      #
      # @overload []=(range, elements)
      #
      #   @param range [Range] range of positions at which to replace elements
      #   @param elements [Array] new elements to replace in this range
      #
      # @overload []=(start_position, count, elements)
      #
      #   @param start_position [Integer] position at which to begin replacing
      #     elements
      #   @param count [Integer] number of elements to replace
      #   @param elements [Array] new elements to replace in this range
      #
      def []=(position, *args)
        if position.is_a?(Range)
          first, count = position.first, position.count
        else
          first, count = position, args[-2]
        end

        element = args[-1] =
          if args[-1].is_a?(Array) then cast_collection(args[-1])
          else cast_element(args[-1])
          end

        if first < 0
          fail ArgumentError,
               "Bad index #{position}: CQL lists do not support negative " \
               "indices"
        end

        to_update do
          if count.nil?
            updater.list_replace(column_name, first, element)
          else
            element = Array.wrap(element)
            count.times do |i|
              if i < element.length
                updater.list_replace(column_name, first+i, element[i])
              else
                deleter.list_remove_at(column_name, first+i)
              end
            end
          end
        end
        to_modify { super }
      end

      #
      # Remove all elements from the list. This will propagate to the database
      # as a DELETE of the list column.
      #
      # @return [List] self
      #
      def clear
        to_update { deleter.delete_columns(column_name) }
        to_modify { super }
      end

      #
      # Concatenate another collection onto this list.
      #
      # @param array [Array] elements to concatenate
      # @return [List] self
      #
      def concat(array)
        array = cast_collection(array)
        to_update { updater.list_append(column_name, array) }
        to_modify { super }
      end

      #
      # Remove all instances of a given value from the list.
      #
      # @param object value to remove
      # @return [List] self
      #
      def delete(object)
        object = cast_element(object)
        to_update { updater.list_remove(column_name, object) }
        to_modify { super }
      end

      #
      # Remove the element at a given position from the list.
      #
      # @param index [Integer] position from which to remove the element
      # @return [List] self
      #
      def delete_at(index)
        to_update { deleter.list_remove_at(column_name, index) }
        to_modify { super }
      end

      #
      # Push (append) one or more elements to the end of the list.
      #
      # @param objects value(s) to add to the end of the list
      # @return [List] self
      #
      def push(*objects)
        objects.map! { |object| cast_element(object) }
        to_update { updater.list_append(column_name, objects) }
        to_modify { super }
      end
      alias_method :<<, :push
      alias_method :append, :push

      #
      # Replace the entire contents of this list with a new collection
      #
      # @param array [Array] new elements for this list
      # @return [List] self
      #
      def replace(array)
        array = cast_collection(array)
        to_update { updater.set(column_name => array) }
        to_modify { super }
      end

      #
      # Prepend one or more values to the beginning of this list
      #
      # @param objects value(s) to add to the beginning of the list
      # @return [List] self
      #
      def unshift(*objects)
        objects.map!(&method(:cast_element))
        to_update { updater.list_prepend(column_name, objects.reverse) }
        to_modify { super }
      end
      alias_method :prepend, :unshift
    end

    #
    # The value of a set column in a {Record} instance. Contains an unordered,
    # unique set of elements. Encapsulates and behaves like the `Set` type from
    # the standard library.
    #
    # @see http://cassandra.apache.org/doc/cql3/CQL.html#set
    #   CQL documentation for set columns
    # @since 1.0.0
    #
    class Set < DelegateClass(::Set)
      include Collection

      # These methods are not implemented because they cannot be expressed as a
      # single CQL3 write operation.
      NON_ATOMIC_MUTATORS = [
        :add?,
        :collect!,
        :delete?,
        :delete_if,
        :flatten!,
        :keep_if,
        :map!,
        :reject!,
        :select!
      ]
      NON_ATOMIC_MUTATORS
        .each { |method| undef_method(method) if method_defined? method }

      #
      # Add an element to the set
      #
      # @param object element to add
      # @return [Set] self
      #
      def add(object)
        object = cast_element(object)
        to_update { updater.set_add(column_name, object) }
        to_modify { super }
      end
      alias_method :<<, :add

      #
      # Remove everything from the set. Equivalent to deleting the collection
      # column from the record's row.
      #
      # @return [Set] self
      #
      def clear
        to_update { deleter.delete_columns(column_name) }
        to_modify { super }
      end

      #
      # Remove a single element from the set
      #
      # @param object element to remove
      # @return [Set] self
      #
      def delete(object)
        object = cast_element(object)
        to_update { updater.set_remove(column_name, object) }
        to_modify { super }
      end

      #
      # Replace the entire contents of this set with another set
      #
      # @param set [::Set] set containing new elements
      # @return [Set] self
      #
      def replace(set)
        set = cast_collection(set)
        to_update { updater.set(column_name => set) }
        to_modify { super }
      end
    end

    #
    # The value of a `map` column in a {Record} instance. Encapsulates and
    # behaves like a built-in `Hash`.
    #
    # @see http://cassandra.apache.org/doc/cql3/CQL.html#map
    #   CQL documentation for map columns
    # @since 1.0.0
    #
    class Map < DelegateClass(::Hash)
      include Collection
      extend Forwardable

      # These methods involve mutation that cannot be expressed as a CQL
      # operation, so are not implemented.
      NON_ATOMIC_MUTATORS = [
        :default,
        :default=,
        :default_proc,
        :default_proc=,
        :delete_if,
        :deep_merge!,
        :except!,
        :extract!,
        :keep_if,
        :reject!,
        :reverse_merge!,
        :reverse_update,
        :select!,
        :shift,
        :slice!,
        :stringify_keys!,
        :symbolize_keys!,
        :to_options!,
        :transform_keys!
      ]
      NON_ATOMIC_MUTATORS
        .each { |method| undef_method(method) if method_defined? method }

      #
      # Set the value of a given key
      #
      # @param key the key
      # @param value the value
      # @return [Map] self
      #
      def []=(key, value)
        key = cast_key(key)
        to_update { updater.map_update(column_name, key => value) }
        to_modify { super }
      end
      alias_method :store, :[]=

      #
      # Remove all elements from this map. Equivalent to deleting the column
      # value from the row in CQL
      #
      # @return [Map] self
      #
      def clear
        to_update { deleter.delete_columns(column_name) }
        to_modify { super }
      end

      #
      # Delete one key from the map
      #
      # @param key the key to delete
      # @return [Map] self
      #
      def delete(key)
        key = cast_key(key)
        to_update { deleter.map_remove(column_name, key) }
        to_modify { super }
      end

      #
      # Update a collection of keys and values given by a hash
      #
      # @param hash [Hash] hash containing keys and values to set
      # @return [Map] self
      #
      def merge!(hash)
        hash = cast_collection(hash)
        to_update { updater.map_update(column_name, hash) }
        to_modify { super }
      end
      alias_method :update, :merge!

      #
      # Replace the entire contents of this map with a new one
      #
      # @param hash [Hash] hash containing new keys and values
      # @return [Map] self
      #
      def replace(hash)
        hash = cast_collection(hash)
        to_update { updater.set(column_name => hash) }
        to_modify { super }
      end

      private

      def_delegator 'column.key_type', :cast, :cast_key
      private :cast_key
    end
  end
end
