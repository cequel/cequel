# -*- encoding : utf-8 -*-
namespace :cequel do
  namespace :keyspace do
    desc 'Initialize Cassandra keyspace'
    task :create => :environment do
      Cequel::Record.connection.schema.create!
      puts "Created keyspace #{Cequel::Record.connection.name}"
    end

    desc 'Drop Cassandra keyspace'
    task :drop => :environment do
      Cequel::Record.connection.schema.drop!
      puts "Dropped keyspace #{Cequel::Record.connection.name}"
    end
  end

  desc "Synchronize all models defined in `app/models' with Cassandra " \
       "database schema"
  task :migrate => :environment do
    watch_stack = ActiveSupport::Dependencies::WatchStack.new

    migration_table_names = Set[]
    models_dir_path = "#{Rails.root.join('app', 'models')}/"
    model_files = Dir.glob(Rails.root.join('app', 'models', '**', '*.rb'))
    model_files.sort.each do |file|
      watch_namespaces = ["Object"]
      model_file_name = file.sub(/^#{Regexp.escape(models_dir_path)}/, "")
      dirname = File.dirname(model_file_name)
      next if dirname == 'concerns' || File.dirname(dirname) == 'concerns'
      watch_namespaces << dirname.classify unless dirname == "."
      watch_stack.watch_namespaces(watch_namespaces)
      require_dependency(file)

      new_constants = watch_stack.new_constants
      if new_constants.empty?
        new_constants << model_file_name.sub(/\.rb$/, "").classify
      end

      new_constants.each do |class_name|
        begin
          clazz = class_name.constantize
        rescue NameError # rubocop:disable HandleExceptions
        else
          if clazz.ancestors.include?(Cequel::Record) &&
              !migration_table_names.include?(clazz.table_name.to_sym)
            clazz.synchronize_schema
            migration_table_names << clazz.table_name.to_sym
            puts "Synchronized schema for #{class_name}"
          end
        end
      end
    end
  end

  desc "Create keyspace and tables for all defined models"
  task :init => %w(keyspace:create migrate)
end
