module Cequel

  module Record

    module Serialization

      extend ActiveSupport::Concern

      module ClassMethods
        def column(name, type, options = {})
          super
          if options[:serialize]
            if options[:serialize] == :json
              def_serialized_raw_accessors(name)
              def_attr_writer_without_cast(name)
            else
              raise ArgumentError, "Invalid option #{options[:serialize].inspect} provided for :serialize. Only :json is currently supported."
            end
          end
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
