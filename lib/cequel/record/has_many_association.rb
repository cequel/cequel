# -*- encoding : utf-8 -*-
module Cequel
  module Record
    #
    # Represents a child association declared by
    # {Associations::ClassMethods#has_many has_many}.
    #
    # @see Associations::ClassMethods#child_associations
    # @since 1.0.0
    #
    class HasManyAssociation
      # @return [Class] Record class that declares this association
      attr_reader :owner_class
      # @return [Symbol] name of this association
      attr_reader :name
      # @return [Symbol] name of the child class that this association contains
      attr_reader :association_class_name
      # @return [Boolean] behavior for propagating destruction from parent to
      #   children
      attr_reader :dependent

      #
      # @param owner_class [Class] Record class that declares this association
      # @param name [Symbol] name of the association
      # @param options [Options] options for the association
      # @option options [Symbol] :class_name name of the child class
      # @option options [Boolean] :dependent propagation behavior for destroy
      #
      # @api private
      #
      def initialize(owner_class, name, options = {})
        options.assert_valid_keys(:class_name, :dependent)

        @owner_class, @name = owner_class, name
        @association_class_name =
          options.fetch(:class_name, name.to_s.classify)
        case options[:dependent]
        when :destroy, :delete, nil
          @dependent = options[:dependent]
        else
          fail ArgumentError,
               "Invalid :dependent option #{options[:dependent].inspect}. " \
               "Valid values are :destroy, :delete"
        end
      end

      #
      # @return [Class] class of child association
      #
      def association_class
        @association_class ||= association_class_name.constantize
      end

      # @private
      def instance_variable_name
        @instance_variable_name ||= :"@#{name}"
      end
    end
  end
end
