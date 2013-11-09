module Cequel

  module Record

    module Callbacks

      extend ActiveSupport::Concern

      included do
        extend ActiveModel::Callbacks
        define_model_callbacks :save, :create, :update, :destroy
      end

      def save(options = {})
        connection.batch { run_callbacks(:save) { super }}
      end

      def destroy
        connection.batch { run_callbacks(:destroy) { super }}
      end

      protected

      def create
        run_callbacks(:create) { super }
      end

      def update
        run_callbacks(:update) { super }
      end

    end

  end

end
