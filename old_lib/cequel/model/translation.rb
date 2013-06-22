module Cequel

  module Model

    module Translation

      extend ActiveModel::Translation

      def i18n_scope
        'cequel'
      end

    end
    
  end

end
