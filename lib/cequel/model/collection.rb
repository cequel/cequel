require 'delegate'

module Cequel

  module Model

    module Collection

      extend ActiveSupport::Concern

      delegate 'loaded?', :to => '@model'

      included do
        private
        define_method(
          :method_missing,
          BasicObject.instance_method(:method_missing))
      end

      def initialize(model, column_name)
        @model, @column_name = model, column_name
        @modifications = Hash.new { |h, k| h[k] = [] }
      end

      def _update(scope)
      end

      protected

      def __getobj__
        @model.__send__(:read_attribute, @column_name) ||
          @model.__send__(:write_attribute, @column_name, self.class.empty)
      end

      def __setobj__(obj)
        raise "Attempted to call __setobj__ on read-only delegate!"
      end

      def modify!(operation, *values)
        @modifications[operation].concat(values)
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
          modify!(:replace, [first, element])
        else
          element = Array.wrap(element)
          count.times do |i|
            if i < element.length
              modify!(:replace, [first+i, element[i]])
            else
              modify!(:remove_at, first+i)
            end
          end
        end
        super if loaded?
      end

      def clear
        modify!(:delete)
        super if loaded?
      end

      def concat(array)
        modify!(:append, array)
        super if loaded?
      end

      def delete(object)
        modify!(:remove, object)
        super if loaded?
      end

      def delete_at(index)
        modify!(:remove_at, index)
        super if loaded?
      end

      def push(object)
        modify!(:append, object)
        super if loaded?
        self
      end
      alias_method :<<, :push

      def replace(array)
        modify! :overwrite, array
        super if loaded?
        self
      end

      def unshift(*objs)
        modify!(:prepend, objs.reverse)
        super if loaded?
        self
      end

      def _update(scope)
        @modifications.each do |type, value|
          case type
          when :append
            scope.list_append(@column_name, value)
          when :prepend
            scope.list_prepend(@column_name, value)
          when :replace
            value.each do |position, element|
              scope.list_replace(@column_name, position, element)
            end
          when :remove_at
            scope.list_remove_at(@column_name, *value)
          when :remove
            scope.list_remove(@column_name, value)
          when :delete
            scope.delete(@column_name)
          when :overwrite
            scope.update(@column_name => value)
          end
        end
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
        modify!(:add, object)
        super if loaded?
        self
      end

      def clear
        modify!(:delete)
        super if loaded?
        self
      end

      def delete(object)
        modify!(:remove, object)
        super if loaded?
        self
      end

      def replace(set)
        modify!(:overwrite, set)
        super
        self
      end

      def _update(scope)
        @modifications.each do |type, value|
          case type
          when :add
            scope.set_add(@column_name, value)
          when :delete
            scope.delete(@column_name)
          when :overwrite
            scope.update(@column_name => value.first)
          when :remove
            scope.set_remove(@column_name, value)
          end
        end
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
        modify!(:update, [key, value])
        super if loaded?
      end
      alias_method :store, :[]=

      def clear
        modify!(:delete)
        super if loaded?
        self
      end

      def delete(key)
        modify!(:remove, key)
        super if loaded?
        self
      end

      def merge!(hash)
        modify!(:update, *hash.to_a)
        super if loaded?
        self
      end
      alias_method :update, :merge!

      def replace(hash)
        modify!(:overwrite, hash)
        super if loaded?
        self
      end

      def _update(scope)
        @modifications.each do |type, value|
          case type
          when :update
            scope.map_update(@column_name, Hash[value])
          when :delete
            scope.delete(@column_name)
          when :overwrite
            scope.update(@column_name => value.first)
          when :remove
            scope.map_remove(@column_name, value)
          end
        end
      end

    end

  end

end
