module Cequel
  module Record
    class Railtie < Rails::Railtie
      config.cequel = Record
      config.cequel_skip_configuration = false

      def self.app_name
        Rails.application.railtie_name.sub(/_application$/, '')
      end

      initializer "cequel.configure_rails" do
        unless config.cequel_skip_configuration
          config_path = Rails.root.join('config/cequel.yml').to_s

          if File.exist?(config_path)
            cequel_config = YAML::load(ERB.new(IO.read(config_path)).result)[Rails.env].
              deep_symbolize_keys
          else
            cequel_config = {host: '127.0.0.1:9160'}
          end
          cequel_config.reverse_merge!(keyspace: "#{Railtie.app_name}_#{Rails.env}")
          connection = Cequel.connect(cequel_config)

          connection.logger = Rails.logger
          Record.connection = connection
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
