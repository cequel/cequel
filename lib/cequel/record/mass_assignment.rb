begin
  require 'active_model/forbidden_attributes_protection'
rescue LoadError
  require 'active_model/mass_assignment_security'
end

module Cequel

  module Record

    module MassAssignment

      extend ActiveSupport::Concern

      included do
        if defined? ActiveModel::ForbiddenAttributesProtection
          include ActiveModel::ForbiddenAttributesProtection
        else
          include ActiveModel::MassAssignmentSecurity
        end
      end

      def attributes=(attributes)
        super(sanitize_for_mass_assignment(attributes))
      end

    end

  end

end
