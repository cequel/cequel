module Cequel

  module Model

    class Railtie < Rails::Railtie

      config.cequel = Cequel::Model::Base

      initializer "cequel.configure_rails" do
        app_name = Rails.application.railtie_name.sub(/_application$/, '')
        config_path = Rails.root.join('config/cequel.yml').to_s

        if File.exist?(config_path)
          config = YAML::load(ERB.new(IO.read(config_path)).result)[Rails.env].
            deep_symbolize_keys
        else
          config = {host: '127.0.0.1:9160'}
        end
        config.reverse_merge!(keyspace: "#{app_name}_#{Rails.env}")
        connection = Cequel.connect(config)

        begin
          connection = Cequel.connect(config)
        rescue CassandraCQL::Error::InvalidRequestException
          connection = Cequel.connect(config.except(:keyspace))
          #XXX This should be read from the configuration
          connection.execute(<<-CQL)
            CREATE KEYSPACE #{keyspace}
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
          CQL
          retry
        end
        connection.logger = Rails.logger
        Cequel::Model::Base.connection = connection
      end
    end

  end

end
