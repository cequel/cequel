module Cequel

  module Model

    # 
    # This is ripped directly off of ActiveRecord::Observer
    #
    class Observer < ActiveModel::Observer

      protected

      def observed_classes
        klasses = super
        klasses + klasses.map { |klass| klass.descendants }.flatten
      end

      def add_observer!(klass)
        super
        define_callbacks klass
      end

      def define_callbacks(klass)
        observer = self
        observer_name = observer.class.name.underscore.gsub('/', '__')

        Cequel::Model::Callbacks::CALLBACKS.each do |callback|
          next unless respond_to?(callback)
          callback_meth = :"_notify_#{observer_name}_for_#{callback}"
          unless klass.respond_to?(callback_meth)
            klass.send(:define_method, callback_meth) do |&block|
              observer.update(callback, self, &block)
            end
            klass.send(callback, callback_meth)
          end
        end
      end

    end
    
  end

end
