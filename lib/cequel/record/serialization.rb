module Cequel

  module Record

    module Serialization

      extend ActiveSupport::Concern

      module ClassMethods
        def column(name, type, options = {})
          super
          def_serialized_raw_accessors(name) if options[:serialize]
          def_attr_writer_without_cast(name) if options[:serialize]
        end

        protected

        def def_serialized_raw_accessors(name)
          module_eval <<-RUBY
            def #{name}_from_raw(value); Oj.load(value); end
            def #{name}_to_raw(value); Oj.dump(value); end
          RUBY
        end

        def def_attr_writer_without_cast(name)
          module_eval <<-RUBY
            def write_attribute(name, value)
              super
              @attributes[name] = value
            end
          RUBY
        end
      end

    end

  end

end
