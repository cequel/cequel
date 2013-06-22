module Cequel

  module Model

    module MassAssignmentSecurity

      extend ActiveSupport::Concern

      included do
        include ActiveModel::MassAssignmentSecurity
      end

      def attributes=(attributes)
        super(sanitize_for_mass_assignment(attributes))
      end

    end
    
  end

end
