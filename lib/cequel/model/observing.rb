module Cequel

  module Model

    extend ActiveModel::Observing::ClassMethods

    module Observing

      extend ActiveSupport::Concern

      included do
        include ActiveModel::Observing
        before_save :_notify_observers_before_save
        after_save :_notify_observers_after_save
        before_create :_notify_observers_before_create
        after_create :_notify_observers_after_create
        before_update :_notify_observers_before_update
        after_update :_notify_observers_after_update
        before_destroy :_notify_observers_before_destroy
        after_destroy :_notify_observers_after_destroy
        before_validation :_notify_observers_before_validation
        after_validation :_notify_observers_after_validation
      end

      private

      def _notify_observers_before_create
        notify_observers(:before_create)
      end

      def _notify_observers_after_create
        notify_observers(:after_create)
      end

      def _notify_observers_before_save
        notify_observers(:before_save)
      end

      def _notify_observers_after_save
        notify_observers(:after_save)
      end

      def _notify_observers_before_update
        notify_observers(:before_update)
      end

      def _notify_observers_after_update
        notify_observers(:after_update)
      end

      def _notify_observers_before_destroy
        notify_observers(:before_destroy)
      end

      def _notify_observers_after_destroy
        notify_observers(:after_destroy)
      end

      def _notify_observers_before_validation
        notify_observers(:before_validation)
      end

      def _notify_observers_after_validation
        notify_observers(:after_validation)
      end

    end
  
  end

end
