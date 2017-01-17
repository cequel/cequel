# -*- encoding : utf-8 -*-
task :environment do
end

namespace :cequel do
  namespace :keyspace do
    desc 'Initialize Cassandra keyspace'
    task :create => :environment do
      create!
    end

    desc 'Initialize Cassandra keyspace if not exist'
    task :create_if_not_exist => :environment do
      if Cequel::Record.connection.schema.exists?
        puts "Keyspace #{Cequel::Record.connection.name} already exists. Nothing to do."
        next
      end
      create!
    end

    desc 'Drop Cassandra keyspace'
    task :drop => :environment do
      drop!
    end

    desc 'Drop Cassandra keyspace if exist'
    task :drop_if_exist => :environment do
      unless Cequel::Record.connection.schema.exists?
        puts "Keyspace #{Cequel::Record.connection.name} doesn't exist. Nothing to do."
        next
      end
      drop!
    end
  end

  desc "Synchronize all models defined in `app/models' with Cassandra " \
       "database schema"
  task :migrate => :environment do
    migrate
  end

  desc "Create keyspace and tables for all defined models"
  task :init => %w(keyspace:create migrate)


  desc 'Drop keyspace if exists, then create and migrate'
  task :reset => :environment do
    if Cequel::Record.connection.schema.exists?
      drop!
    end
    create!
    migrate
  end

  class NoModelDirectoryFound < StandardError; end

  def create!
    Cequel::Record.connection.schema.create!
    puts "Created keyspace #{Cequel::Record.connection.name}"
  end


  def drop!
    Cequel::Record.connection.schema.drop!
    puts "Dropped keyspace #{Cequel::Record.connection.name}"
  end

  #
  require File.expand_path('./concerns/cassandra_wordsmaster_list', File.dirname(__FILE__))

  def models_dir_path
    models_dir_path = ENV['CEQUEL_MODELS_PATH']
    models_dir_path = Pathname.expand_path('app/models', Rails.root)
                        if defined?(Rails::Railtie) && models_dir_path.nil?
    models_dir_path = Pathname.expand_path('app/models', Dir.pwd)
                        if models_dir_path.nil?
    raise NoModelDirectoryFound, "#{model_dir_path} is not a directory" unless Pathname.new(models_dir_path).directory?
    models_dir_path
  end

  def migrate
    watch_stack = ActiveSupport::Dependencies::WatchStack.new
    migration_table_names = Set[]
    model_files = Dir.glob(File.join(models_dir_path, '**', '*.rb'))
    model_files.sort.each do |file|
      watch_namespaces = ["Object"]
      model_file_name = file.sub(/^#{Regexp.escape(models_dir_path)}/, "")
      dirname = File.dirname(model_file_name)
      watch_namespaces << dirname.classify unless dirname == "."
      watch_stack.watch_namespaces(watch_namespaces)
      require_dependency(file)

      new_constants = watch_stack.new_constants
      if new_constants.empty?
        new_constants << model_file_name.sub(/\.rb$/, "").classify
      end

      new_constants.each do |class_name|
        # rubocop:disable HandleExceptions
        begin
          clazz = class_name.constantize
        rescue LoadError, NameError, RuntimeError
        else
          if clazz.is_a?(Class)
            if clazz.ancestors.include?(Cequel::Record) &&
                !migration_table_names.include?(clazz.table_name.to_sym)
              clazz.synchronize_schema
              migration_table_names << clazz.table_name.to_sym
              puts "Synchronized schema for #{class_name}"
            end
          end
        end
        # rubocop:enable HandleExceptions
      end
    end
  end
end
