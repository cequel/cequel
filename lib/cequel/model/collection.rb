require 'delegate'

module Cequel

  module Model

    module Collection

      extend ActiveSupport::Concern
      extend Forwardable

      def_delegators :@model, :loaded?, :updater, :deleter

      attr_reader :column_name

      included do
        private
        define_method(
          :method_missing,
          BasicObject.instance_method(:method_missing))
      end

      def initialize(model, column_name)
        @model, @column_name = model, column_name
      end

      protected

      def __getobj__
        @model.__send__(:read_attribute, @column_name) ||
          @model.__send__(:write_attribute, @column_name, self.class.empty)
      end

      def __setobj__(obj)
        raise "Attempted to call __setobj__ on read-only delegate!"
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
      NON_ATOMIC_MUTATORS.each { |method| undef_method(method) }

      def self.empty; []; end

      def []=(position, *args)
        if Range === position then first, count = position.first, position.count
        else first, count = position, args[-2]
        end
        element = args[-1]
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
        super if loaded?
      end

      def clear
        deleter.delete_columns(column_name)
        super if loaded?
      end

      def concat(array)
        updater.list_append(column_name, array)
        super if loaded?
      end

      def delete(object)
        updater.list_remove(column_name, object)
        super if loaded?
      end

      def delete_at(index)
        deleter.list_remove_at(column_name, index)
        super if loaded?
      end

      def push(object)
        updater.list_append(column_name, object)
        super if loaded?
        self
      end
      alias_method :<<, :push

      def replace(array)
        updater.set(column_name => array)
        super if loaded?
        self
      end

      def unshift(*objs)
        updater.list_prepend(column_name, objs.reverse)
        super if loaded?
        self
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
      NON_ATOMIC_MUTATORS.each { |method| undef_method(method) }

      def add(object)
        updater.set_add(column_name, object)
        super if loaded?
        self
      end

      def clear
        deleter.delete_columns(column_name)
        super if loaded?
        self
      end

      def delete(object)
        updater.set_remove(column_name, object)
        super if loaded?
        self
      end

      def replace(set)
        updater.set(column_name => set)
        super
        self
      end

    end

    class Map < DelegateClass(::Hash)

      include Collection

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
      NON_ATOMIC_MUTATORS.each { |method| undef_method(method) }

      def []=(key, value)
        updater.map_update(column_name, key => value)
        super if loaded?
      end
      alias_method :store, :[]=

      def clear
        deleter.delete_columns(column_name)
        super if loaded?
        self
      end

      def delete(key)
        deleter.map_remove(column_name, key)
        super if loaded?
        self
      end

      def merge!(hash)
        updater.map_update(column_name, hash)
        super if loaded?
        self
      end
      alias_method :update, :merge!

      def replace(hash)
        updater.set(column_name => hash)
        super if loaded?
        self
      end

    end

  end

end
