module Cequel
  
  module Model

    module Timestamps

      extend ActiveSupport::Concern

      included do
        include CreatedAt
        include UpdatedAt
      end

      module CreatedAt

        extend ActiveSupport::Concern

        included do
          column :created_at, :timestamp
          before_create :_set_created_at
        end

        private

        def _set_created_at
          self.created_at = Time.now
        end

      end

      module UpdatedAt

        extend ActiveSupport::Concern

        included do
          column :updated_at, :timestamp
          before_save :_set_updated_at
        end

        private

        def _set_updated_at
          self.updated_at = Time.now
        end

      end

    end

  end

end
