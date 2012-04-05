module Cequel

  module Model

    module Properties

      extend ActiveSupport::Concern

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
          RUBY
        end

        def column(name, type)
          name = name.to_sym
          @_cequel.add_column(name, type)

          module_eval <<-RUBY, __FILE__, __LINE__+1
            def #{name}
              read_attribute(#{name.inspect})
            end

            def #{name}=(value)
              write_attribute(#{name.inspect}, value)
            end
          RUBY
        end

        def key_alias
          @_cequel.key.name
        end

        def column_names
          [@_cequel.key.name, *@_cequel.columns.keys]
        end

        def columns
          [@_cequel.key, *@_cequel.columns.values]
        end

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
