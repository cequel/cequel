# -*- encoding : utf-8 -*-
module Cequel
  module Record
    #
    # Cequel::Record models provide lifecycle callbacks for `create`, `update`,
    # `save`, `destroy`, and `validation`.
    #
    # @example
    #   class User
    #     include Cequel::Record
    #
    #     key :login, :text
    #     column :name, :text
    #
    #     after_create :send_welcome_email
    #     after_update :reindex_posts_for_search
    #     after_save :reindex_for_search
    #     after_destroy :send_farewell_email
    #     before_validation :set_permalink
    #   end
    #
    # @since 0.1.0
    #
    module Callbacks
      extend ActiveSupport::Concern

      included do
        extend ActiveModel::Callbacks
        define_model_callbacks :save, :create, :update, :destroy
      end

      # (see Persistence#save)
      def save(options = {})
        connection.batch(options.slice(:consistency)) do
          run_callbacks(:save) { super }
        end
      end

      # (see Persistence#destroy)
      def destroy(options = {})
        connection.batch(options.slice(:consistency)) do
          run_callbacks(:destroy) { super }
        end
      end

      protected

      def create(*)
        run_callbacks(:create) { super }
      end

      def update(*)
        run_callbacks(:update) { super }
      end
    end
  end
end
