module Cequel

  module Model

    module Properties

      extend ActiveSupport::Concern

      module ClassMethods

        def key(key_alias, type)
          @_cequel.key = Column.new(key_alias.to_sym, type)

          module_eval(<<-RUBY, __FILE__, __LINE__+1)
            def #{key_alias}
              @_cequel.key
            end

            def #{key_alias}=(key)
              @_cequel.key = key
            end
          RUBY
        end

        def key_alias
          @_cequel.key.try(:name)
        end

      end

    end

  end

end
