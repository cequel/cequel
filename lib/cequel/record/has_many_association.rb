module Cequel

  module Record

    class HasManyAssociation

      attr_reader :owner_class, :name, :association_class_name, :dependent

      def initialize(owner_class, name, options = {})
        options.assert_valid_keys(:class_name, :dependent)

        @owner_class, @name = owner_class, name
        @association_class_name = options.fetch(:class_name, name.to_s.classify)
        case options[:dependent]
        when :destroy, :delete, nil
          @dependent = options[:dependent]
        else
          raise ArgumentError,
            "Invalid :dependent option #{options[:dependent].inspect}." +
              "Valid values are :destroy, :delete"
        end
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
