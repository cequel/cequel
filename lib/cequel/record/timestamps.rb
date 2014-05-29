# -*- encoding : utf-8 -*-
module Cequel
  module Record
    module Timestamps
      extend ActiveSupport::Concern

      module ClassMethods
        def timestamps
          self.class_eval do
            column :created_at, :timestamp
            column :updated_at, :timestamp

            before_create :_set_created_and_updated_at
            before_update :_set_updated_at
          end
        end
      end

      private

      def _set_created_and_updated_at
        t = Time.now
        self.created_at = t
        self.updated_at = t
      end

      def _set_updated_at
        self.updated_at = Time.now
      end

    end
  end
end