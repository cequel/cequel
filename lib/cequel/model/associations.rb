module Cequel

  module Model

    module Associations

      extend ActiveSupport::Concern

      module ClassMethods

        def belongs_to(name, options = {})
          name = name.to_sym
          association = LocalAssociation.new(name, self, options.symbolize_keys)
          @_cequel.associations[name] = association
          column(association.foreign_key_name, association.primary_key.type)

          module_eval <<-RUBY, __FILE__, __LINE__+1
            def #{name}
              if @_cequel.associations.key?(#{name.inspect})
                return @_cequel.associations[#{name.inspect}]
              end
              key = __send__(:#{name}_id)
              if key
                @_cequel.associations[#{name.inspect}] =
                  self.class.reflect_on_association(#{name.inspect}).
                    scope(self).first
              else
                @_cequel.associations[#{name.inspect}] = nil
              end
            end

            def #{association.foreign_key_name}=(key)
              @_cequel.associations.delete(#{name.inspect})
              write_attribute(#{association.foreign_key_name.inspect}, key)
            end
          RUBY
        end

        def has_many(name, options = {})
          name = name.to_sym
          @_cequel.associations[name] =
            RemoteAssociation.new(name, self, options.symbolize_keys)
          
          module_eval <<-RUBY, __FILE__, __LINE__+1
            def #{name}
              self.class.reflect_on_association(#{name.inspect}).scope(self)
            end
          RUBY
        end

        def has_one(name, options = {})
          name = name.to_sym
          @_cequel.associations[name] =
            RemoteAssociation.new(name, self, options.symbolize_keys)

          module_eval <<-RUBY, __FILE__, __LINE__+1
            def #{name}
              self.class.reflect_on_association(#{name.inspect}).scope(self).first
            end
          RUBY
        end

        def reflect_on_association(name)
          @_cequel.associations[name.to_sym]
        end

      end

    end
    
  end

end
