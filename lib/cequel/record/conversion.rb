# -*- encoding : utf-8 -*-
module Cequel
  module Record
    #
    # Provides support for ActiveModel::Conversions
    #
    module Conversion
      extend ActiveSupport::Concern

      included do
        include ActiveModel::Conversion
        alias_method :to_key, :key_values
      end
    end
  end
end
