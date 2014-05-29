# -*- encoding : utf-8 -*-
module Cequel
  module Record
    module Timestamps
      extend ActiveSupport::Concern

      module ClassMethods
        def timestamps
          column :created_at, :timestamp
          column :updated_at, :timestamp

          before_create :set_created_and_updated_at
          before_update :set_updated_at
        end
      end

      private

      def set_created_and_updated_at
        now = Time.now
        self.created_at = now
        self.updated_at = now
      end

      def set_updated_at
        self.updated_at = Time.now
      end
    end
  end
end
