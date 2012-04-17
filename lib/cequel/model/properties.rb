module Cequel

  module Model

    module Properties

      extend ActiveSupport::Concern

      included do
        include ActiveModel::Conversion
      end

      module ClassMethods

        def key(key_alias, type)
          key_alias = key_alias.to_sym
          @_cequel.key = Column.new(key_alias, type)

          module_eval(<<-RUBY, __FILE__, __LINE__+1)
            def #{key_alias}
              @_cequel.key
            end

            def #{key_alias}=(key)
              @_cequel.key = key
            end

            def to_key
              [@_cequel.key]
            end
          RUBY
        end

        def column(name, type, options = {})
          name = name.to_sym
          @_cequel.add_column(name, type, options.symbolize_keys)

          module_eval <<-RUBY, __FILE__, __LINE__+1
            def #{name}
              read_attribute(#{name.inspect})
            end

            def #{name}=(value)
              write_attribute(#{name.inspect}, value)
            end
          RUBY

          if type == :boolean
            module_eval <<-RUBY, __FILE__, __LINE__+1 if type == :boolean
              def #{name}?
                !!read_attribute(#{name.inspect})
              end
            RUBY
          end
        end

        def key_alias
          key_column.name
        end

        def key_column
          @_cequel.key
        end

        def column_names
          [@_cequel.key.name, *@_cequel.columns.keys]
        end

        def columns
          [@_cequel.key, *@_cequel.columns.values]
        end

        def type_column
          @_cequel.type_column
        end

      end

      def initialize(attributes = {})
        super()
        self.class.columns.each do |column|
          default = column.default
          @_cequel.attributes[column.name] = default unless default.nil?
        end
        self.attributes = attributes
        yield self if block_given?
      end

      def attributes
        {self.class.key_alias => @_cequel.key}.with_indifferent_access.
          merge(@_cequel.attributes)
      end

      def attributes=(attributes)
        attributes.each_pair do |column_name, value|
          __send__("#{column_name}=", value)
        end
      end

      def ==(other)
        return false if self.class != other.class
        self_key = self.__send__(self.class.key_column.name)
        other_key = other.__send__(self.class.key_column.name)
        self_key && other_key && self_key == other_key
      end

      def inspect
        inspected = "#<#{self.class.name} #{attributes.map { |column, value| "#{column}:#{value.inspect}" }.join(' ')}>"
      end

      private

      def write_attribute(column_name, value)
        if value.nil?
          @_cequel.attributes.delete(column_name)
        else
          @_cequel.attributes[column_name] = value
        end
      end

      def read_attribute(column_name)
        @_cequel.attributes[column_name.to_sym]
      end

    end

  end

end
