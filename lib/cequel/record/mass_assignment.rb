# -*- encoding : utf-8 -*-
begin
  require 'active_model/forbidden_attributes_protection'
rescue LoadError
  require 'active_model/mass_assignment_security'
end

module Cequel
  module Record
    # rubocop:disable LineLength

    #
    # Cequel supports mass-assignment protection in both the Rails 3 and Rails
    # 4 paradigms. Rails 3 applications may define `attr_protected` and
    # `attr_accessible` attributes in {Record} classes. In Rails 4, Cequel will
    # respect strong parameters.
    #
    # @see https://github.com/rails/strong_parameters Rails 4 Strong Parameters
    # @see
    #   http://api.rubyonrails.org/v3.2.15/classes/ActiveModel/MassAssignmentSecurity.html
    #   Rails 3 mass-assignment security
    #
    # @since 1.0.0
    #
    module MassAssignment
      # rubocop:enable LineLength
      extend ActiveSupport::Concern

      included do
        if defined? ActiveModel::ForbiddenAttributesProtection
          include ActiveModel::ForbiddenAttributesProtection
        else
          include ActiveModel::MassAssignmentSecurity
        end
      end

      # @private
      def attributes=(attributes)
        super(sanitize_for_mass_assignment(attributes))
      end
    end
  end
end
