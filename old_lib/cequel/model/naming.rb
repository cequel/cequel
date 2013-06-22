module Cequel

  module Model

    module Naming

      extend ActiveSupport::Concern

      included do
        include ActiveModel::Naming
      end

    end

  end

end
