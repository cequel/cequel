require 'delegate'

module Cequel

  module Record

    module Collection

      extend ActiveSupport::Concern
      extend Forwardable

      def_delegators :@model, :loaded?, :updater, :deleter
      def_delegator :@column, :name, :column_name
      def_delegators :__getobj__, :clone, :dup

      included do
        private
        define_method(
          :method_missing,
          BasicObject.instance_method(:method_missing))
      end

      def initialize(model, column)
        @model, @column = model, column
      end

      def inspect
        __getobj__.inspect
      end

      def loaded!
        modifications.each { |modification| modification.() }.clear
      end

      def persisted!
        modifications.clear
      end

      protected

      def __getobj__
        model.__send__(:read_attribute, column_name) ||
          model.__send__(:write_attribute, column_name, self.class.empty)
      end

      def __setobj__(obj)
        raise "Attempted to call __setobj__ on read-only delegate!"
      end

      private
      attr_reader :model, :column
      def_delegator :column, :cast, :cast_collection
      def_delegator 'column.type', :cast, :cast_element
      private :cast_collection, :cast_element

      def to_modify(&block)
        if loaded?
          model.__send__("#{column_name}_will_change!")
          block.()
        else modifications << block
        end
        self
      end

      def modifications
        @modifications ||= []
      end

    end

    class List < DelegateClass(Array)

      include Collection

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
      NON_ATOMIC_MUTATORS.
        each { |method| undef_method(method) if method_defined? method }

      def self.empty; []; end

      def []=(position, *args)
        if Range === position then first, count = position.first, position.count
        else first, count = position, args[-2]
        end

        element = args[-1] =
          if args[-1].is_a?(Array) then cast_collection(args[-1])
          else cast_element(args[-1])
          end

        if first < 0
          raise ArgumentError,
            "Bad index #{position}: CQL lists do not support negative indices"
        end

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
        to_modify { super }
      end

      def clear
        deleter.delete_columns(column_name)
        to_modify { super }
      end

      def concat(array)
        array = cast_collection(array)
        updater.list_append(column_name, array)
        to_modify { super }
      end

      def delete(object)
        object = cast_element(object)
        updater.list_remove(column_name, object)
        to_modify { super }
      end

      def delete_at(index)
        deleter.list_remove_at(column_name, index)
        to_modify { super }
      end

      def push(object)
        object = cast_element(object)
        updater.list_append(column_name, object)
        to_modify { super }
      end
      alias_method :<<, :push

      def replace(array)
        array = cast_collection(array)
        updater.set(column_name => array)
        to_modify { super }
      end

      def unshift(*objs)
        objs.map!(&method(:cast_element))
        updater.list_prepend(column_name, objs.reverse)
        to_modify { super }
      end

    end

    class Set < DelegateClass(::Set)

      include Collection

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
      NON_ATOMIC_MUTATORS.
        each { |method| undef_method(method) if method_defined? method }

      def add(object)
        object = cast_element(object)
        updater.set_add(column_name, object)
        to_modify { super }
      end
      alias_method :<<, :add

      def clear
        deleter.delete_columns(column_name)
        to_modify { super }
      end

      def delete(object)
        object = cast_element(object)
        updater.set_remove(column_name, object)
        to_modify { super }
      end

      def replace(set)
        set = cast_collection(set)
        updater.set(column_name => set)
        to_modify { super }
      end

    end

    class Map < DelegateClass(::Hash)

      include Collection
      extend Forwardable

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
      NON_ATOMIC_MUTATORS.
        each { |method| undef_method(method) if method_defined? method }

      def []=(key, value)
        key = cast_key(key)
        updater.map_update(column_name, key => value)
        to_modify { super }
      end
      alias_method :store, :[]=

      def clear
        deleter.delete_columns(column_name)
        to_modify { super }
      end

      def delete(key)
        key = cast_key(key)
        deleter.map_remove(column_name, key)
        to_modify { super }
      end

      def merge!(hash)
        hash = cast_collection(hash)
        updater.map_update(column_name, hash)
        to_modify { super }
      end
      alias_method :update, :merge!

      def replace(hash)
        hash = cast_collection(hash)
        updater.set(column_name => hash)
        to_modify { super }
      end

      private
      def_delegator 'column.key_type', :cast, :cast_key
      private :cast_key

    end

  end

end
