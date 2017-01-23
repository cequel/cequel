module Cequel
  #
  # Generic module which enables injection of ActiveSupport notification
  # functionality into including classes
  #
  module Instrumentation
    #
    # Metaprogramming method to wrap an existing method with instrumentation
    #
    module ModuleMethods
      # Instruments `method_name` to publish the value returned by the
      # `data_builder` proc onto `topic`
      #
      # Example:
      #
      #    extend Instrumentation
      #    instrument :create, data: {topic: "create.cequel", table_name: table_name}
      #
      # @param method_name [Symbol,String] The method to instrument
      #
      # @param opts [String] :topic ("#{method_name}.cequel") The name
      #   with which to publish this instrumentation
      #
      # @option opts [Object] :data_method (nil) the data to publish along
      #   with the notification. If it responds to `#call` it will be
      #   called with the record object and the return value used for
      #   each notification.
      def instrument(method_name, opts)
        data = opts[:data]
        topic = opts.fetch(:topic, "#{method_name}.cequel")

        data_proc = if data.respond_to? :call
                      data
                    else
                      ->(_) { data }
                    end

        define_method(:"__data_for_#{method_name}_instrumentation", &data_proc)

        mod = Module.new
        mod.module_eval <<-METH
          def #{method_name}(*args)
            instrument("#{topic}",
                       __data_for_#{method_name}_instrumentation(self)) do
              super(*args)
            end
          end
        METH

        prepend mod
      end
    end

    protected

    def instrument(name, data, &blk)
      ActiveSupport::Notifications.instrument(name, data, &blk)
    end

    # Module Methods

    def self.included(a_module)
      a_module.extend ModuleMethods
    end
  end
end
