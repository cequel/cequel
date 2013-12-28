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
        config_path = Rails.root.join('config/cequel.yml').to_s

        if File.exist?(config_path)
          config = YAML::load(ERB.new(IO.read(config_path)).result)[Rails.env].
            deep_symbolize_keys
        else
          config = {host: '127.0.0.1:9160'}
        end
        config.reverse_merge!(keyspace: "#{Railtie.app_name}_#{Rails.env}")
        connection = Cequel.connect(config)

        connection.logger = Rails.logger
        Record.connection = connection
      end

      initializer "cequel.add_new_relic" do
        begin
          require 'new_relic/agent/method_tracer'
        rescue LoadError => e
        else
          require 'cequel/metal/new_relic_instrumentation'
        end
      end

      rake_tasks do
        require "cequel/record/tasks"
      end

      generators do
        require 'cequel/record/configuration_generator.rb'
      end
    end
  end
end
