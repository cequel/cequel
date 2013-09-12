module Cequel

  module Model

    module Validations

      extend ActiveSupport::Concern

      included do
        include ActiveModel::Validations
      end

      def save(options = {})
        validate = options.fetch(:validate, true)
        options.delete(:validate)
        (!validate || valid?) && super
      end

      def save!(options = {})
        tap do
          unless save(options)
            raise RecordInvalid, errors.full_messages.join("; ")
          end
        end
      end

    end

  end

end
