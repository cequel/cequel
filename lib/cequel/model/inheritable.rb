module Cequel

  module Model

    module Inheritable

      module SubclassMethods

        extend ActiveSupport::Concern

        module ClassMethods

          def all
            super.where(@_cequel.type_column.name => name)
          end

        end

        def initialize(*args, &block)
          super
          __send__("#{self.class.type_column.name}=", self.class.name)
        end

      end

      def inherited(subclass)
        super
        unless @_cequel.type_column
          raise ArgumentError,
            "Can't subclass model class that does not define a type column"
        end
        subclass._cequel = SubclassInternals.new(subclass, @_cequel)
        subclass.module_eval { include(SubclassMethods) }
      end

      protected

      attr_writer :_cequel

    end

  end

end
