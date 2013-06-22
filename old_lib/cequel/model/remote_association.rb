module Cequel

  module Model

    class RemoteAssociation

      attr_reader :name, :class_name, :dependent

      def initialize(name, owning_class, options)
        @name, @owning_class = name, owning_class
        @class_name = options[:class_name] || name.to_s.classify.to_sym
        @foreign_key_name = options[:foreign_key]
        @dependent = options[:dependent]
      end

      def scope(instance)
        clazz.where(foreign_key_name => instance.__send__(primary_key_name))
      end

      def primary_key
        @primary_key ||= @owning_class.key_column
      end

      def primary_key_name
        @primary_key_name ||= primary_key.name
      end

      def foreign_key_name
        @foreign_key_name ||= :"#{@owning_class.name.underscore}_id"
      end

      def clazz
        @clazz ||= class_name.to_s.constantize
      end

    end

  end

end
