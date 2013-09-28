module Cequel

  module Record

    class HasManyAssociation

      attr_reader :owner_class, :name, :association_class_name

      def initialize(owner_class, name, options = {})
        @owner_class, @name = owner_class, name
        @association_class_name = options.fetch(:class_name, name.to_s.classify)
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
