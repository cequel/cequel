module Cequel

  module Model

    class Railtie < Rails::Railtie

      config.cequel = Cequel::Model

      initializer "cequel.configure_rails" do
        config_path = Rails.root.join('config/cequel.yml').to_s

        if File.exist?(config_path)
          yaml = YAML.load_file(config_path)[Rails.env]
          Cequel::Model.configure(yaml) if yaml
        end
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
