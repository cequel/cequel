module Cequel

  module Record

    class BelongsToAssociation

      extend Forwardable

      attr_reader :owner_class, :name, :association_class_name

      def_delegator :association_class, :key_columns, :association_key_columns

      def initialize(owner_class, name, options = {})
        @owner_class, @name = owner_class, name.to_sym
        @association_class_name =
          options.fetch(:class_name, @name.to_s.classify)
      end

      def association_class
        @association_class ||= association_class_name.constantize
      end

      def instance_variable_name
        @instance_variable_name ||= :"@#{name}"
      end

    end

  end

end
