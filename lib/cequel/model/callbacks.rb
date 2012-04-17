module Cequel

  module Model

    module Callbacks

      extend ActiveSupport::Concern

      HOOKS = [:save, :create, :update, :destroy, :validation]
      CALLBACKS = HOOKS.map { |hook| [:"before_#{hook}", :"after_#{hook}"] }.
        flatten

      included do
        extend ActiveModel::Callbacks
        define_model_callbacks *HOOKS
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
