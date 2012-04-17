module Cequel
  
  module Model

    module Validations

      extend ActiveSupport::Concern

      included do
        include ActiveModel::Validations
        alias_method_chain :valid?, :callbacks # XXX is there no better way?
      end

      module ClassMethods

        def create!(attributes = {})
          instance = new(attributes)
          instance.save!
        end

      end

      def save(*args)
        if valid?
          super
          true
        else
          false
        end
      end

      def save!(*args)
        raise RecordInvalid unless save
        self
      end

      def valid_with_callbacks?
        run_callbacks(:validation) { valid_without_callbacks? }
      end

    end

  end

end
