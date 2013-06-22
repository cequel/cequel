module Cequel

  module Model

    module Properties

      extend ActiveSupport::Concern

      module ClassMethods

        protected

        def key(name, type)
          schema.add_key(name, type)
        end

        def column(name, type, options = {})
          schema.add_column(name, type, options[:index])
        end

        def list(name, type)
          schema.add_list(name, type)
        end

        def set(name, type)
          schema.add_set(name, type)
        end

        def map(name, key_type, value_type)
          schema.add_map(name, key_type, value_type)
        end

        def table_property(name, value)
          schema.add_property(name, value)
        end

        public

        def read_property(name)
          schema.property(name)
        end

      end

    end

  end

end
