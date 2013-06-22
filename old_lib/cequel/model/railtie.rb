module Cequel

  module Model

    class Railtie < Rails::Railtie

      config.cequel = Cequel::Model

      initializer "cequel.configure_rails" do
        config_path = Rails.root.join('config/cequel.yml').to_s

        if File.exist?(config_path)
          yaml = YAML::load(ERB.new(IO.read(config_path)).result)[Rails.env]
          Cequel::Model.configure(yaml.symbolize_keys) if yaml
        end

        Cequel::Model.logger = Rails.logger
      end

      initializer "cequel.instantiate_observers" do
        config.after_initialize do
          ::Cequel::Model.instantiate_observers

          ActionDispatch::Callbacks.to_prepare do
            ::Cequel::Model.instantiate_observers
          end
        end
      end
    end

  end

end
