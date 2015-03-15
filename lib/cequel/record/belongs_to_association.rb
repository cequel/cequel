# -*- encoding : utf-8 -*-
module Cequel
  module Record
    #
    # Represents a parent association declared by
    # {Associations::ClassMethods#belongs_to belongs_to}
    #
    # @see Associations::ClassMethods#parent_association
    # @since 1.0.0
    #
    class BelongsToAssociation
      extend Util::Forwardable

      # @return [Class] child class that declared `belongs_to`
      attr_reader :owner_class
      # @return [Symbol] name of the association
      attr_reader :name
      # @return [String] name of parent class
      attr_reader :association_class_name

      # @!attribute [r] association_key_columns
      #   @return [Array<Schema::Column>] key columns on the parent class
      def_delegator :association_class, :key_columns, :association_key_columns

      #
      # @param owner_class [Class] child class that declared `belongs_to`
      # @param name [Symbol] name of the association
      # @param options [Options] options for association
      # @option options [String] :class_name name of parent class
      #
      # @api private
      #
      def initialize(owner_class, name, options = {})
        options.assert_valid_keys(:class_name)

        @owner_class, @name = owner_class, name.to_sym
        @association_class_name =
          options.fetch(:class_name, @name.to_s.classify)
      end

      #
      # @return [Class] parent class declared by `belongs_to`
      #
      def association_class
        @association_class ||= association_class_name.constantize
      end

      #
      # @return [Symbol] instance variable name to use for storing the parent
      #   instance in a record
      #
      # @api private
      #
      def instance_variable_name
        @instance_variable_name ||= :"@#{name}"
      end
    end
  end
end
