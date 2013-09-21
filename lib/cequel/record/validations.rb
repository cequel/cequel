module Cequel

  module Record

    module Validations

      extend ActiveSupport::Concern

      included do
        include ActiveModel::Validations
        define_model_callbacks :validation
        alias_method_chain :valid?, :callbacks
      end

      module ClassMethods

        def create!(attributes = {}, &block)
          new(attributes, &block).save!
        end

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

      def update_attributes!(attributes)
        self.attributes = attributes
        save!
      end

      def valid_with_callbacks?
        run_callbacks(:validation) { valid_without_callbacks? }
      end

    end

  end

end
