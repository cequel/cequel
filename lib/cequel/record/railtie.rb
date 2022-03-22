# -*- encoding : utf-8 -*-
require 'active_support/core_ext/hash'
require 'yaml'
require 'erb'

module Cequel
  module Record
    # @private
    # @since 0.1.0
    class Railtie < Rails::Railtie
      config.cequel = Record

      def self.app_name
        Rails.application.railtie_name.sub(/_application$/, '')
      end

      initializer "cequel.configure_rails" do
        connection = Cequel.connect(configuration)

        connection.logger = Rails.logger
        Record.connection = connection
      end

      initializer "cequel.add_new_relic" do
        if configuration.fetch(:newrelic, true)
          begin
            require 'new_relic/agent/datastores'
          rescue LoadError => e
            Rails.logger.debug(
              "New Relic not installed; skipping New Relic integration")
          else
            require 'cequel/metal/new_relic_instrumentation'
          end
        end
      end

      rake_tasks do
        require "cequel/record/tasks"
      end

      generators do
        require 'cequel/record/configuration_generator'
        require 'cequel/record/record_generator'
      end

      private

      def configuration
        return @configuration if defined? @configuration

        config_path = Rails.root.join('config/cequel.yml').to_s

        if File.exist?(config_path)
          config_yaml = ERB.new(File.read(config_path)).result
          @configuration = YAML.load(config_yaml)[Rails.env]
            .deep_symbolize_keys
        else
          @configuration = {host: '127.0.0.1:9042'}
        end
        @configuration
          .reverse_merge!(keyspace: "#{Railtie.app_name}_#{Rails.env}")

        @configuration
      end
    end
  end
end
