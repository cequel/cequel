require 'active_model/callbacks'

module Cequel

  module Model

    module Callbacks

      extend ActiveSupport::Concern

      included do
        extend ActiveModel::Callbacks
        define_model_callbacks :save, :create, :update, :destroy
      end

      def save(*args)
        run_callbacks(:save) do
          run_callbacks(persisted? ? :update : :create) { super }
        end
      end

      def destroy(*args)
        run_callbacks(:destroy) { super }
      end

    end

  end

end
