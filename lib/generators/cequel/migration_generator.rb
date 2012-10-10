require 'rails/generators'
require 'rails/generators/named_base'
require 'cequel/migrator'

module Cequel
  class MigrationGenerator < Rails::Generators::NamedBase

    source_root File.expand_path("../templates", __FILE__)

    def self.banner
      "rails g cequel:migration NAME"
    end

    def self.desc(description = nil)
<<EOF
Description:
  Create an empty Cassandra migration file in '#{Cequel::Migrator.migration_directory}'.

Example:
  `rails g cequel:migration CreateBigTable`
EOF
    end

    def create
      timestamp = Time.now.utc.strftime("%Y%m%d%H%M%S")
      template 'migration.rb.erb', "#{Cequel::Migrator.migration_directory}/#{timestamp}_#{file_name.underscore}.rb"
    end
  end
end
