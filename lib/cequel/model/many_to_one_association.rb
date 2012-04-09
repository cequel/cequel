module Cequel

  module Model

    class ManyToOneAssociation

      attr_reader :clazz, :name

      def initialize(name, owning_class, options)
        @name, @owning_class = name, owning_class
        @class_name = options[:class_name] || name.to_s.classify.to_sym
      end

      def primary_key
        @primary_key ||= clazz.key_column
      end

      def primary_key_name
        @primary_key_name ||= primary_key.name
      end

      def foreign_key_name
        @foreign_key_name ||= :"#{name}_id"
      end

      def scope(instance)
        clazz.where(primary_key_name => instance.__send__(foreign_key_name))
      end

      def clazz
        @clazz ||= @class_name.to_s.constantize
      end
    
    end

  end

end
