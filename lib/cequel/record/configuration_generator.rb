module Cequel
  module Record
    class ConfigurationGenerator < Rails::Generators::Base
      namespace 'cequel:configuration'
      source_root File.expand_path('../../../../templates/', __FILE__)

      def create_configuration
        template "config/cequel.yml"
      end
    end
  end
end
