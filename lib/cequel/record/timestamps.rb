# -*- encoding : utf-8 -*-
module Cequel
  module Record
    module Timestamps
      extend ActiveSupport::Concern

      module ClassMethods
        def key(name, type, options = {})
          super
          if type == :timeuuid && options[:auto]
            module_eval(<<-RUBY, __FILE__, __LINE__+1)
              def created_at
                read_attribute(#{name.inspect}).try(:to_time)
              end
            RUBY
          end
        end

        def timestamps
          column :updated_at, :timestamp

          if method_defined?(:created_at)
            before_save :set_updated_at
          else
            column :created_at, :timestamp

            before_create :set_created_and_updated_at
            before_update :set_updated_at
          end
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
