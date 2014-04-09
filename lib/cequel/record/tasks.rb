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

    path_length = Rails.root.join('app', 'models').to_s.size
    Dir.glob(Rails.root.join('app', 'models', '**', '*.rb')).each do |file|
      watch_stack.watch_namespaces([Object])

      require_dependency(file)

      new_constants = watch_stack.new_constants
      if new_constants.empty?
        base_name = file[path_length...-3]
        new_constants << base_name.classify
      end

      new_constants.each do |class_name|
        begin
          clazz = class_name.constantize
        rescue NameError # rubocop:disable HandleExceptions
        else
          if clazz.ancestors.include?(Cequel::Record)
            clazz.synchronize_schema
            puts "Synchronized schema for #{class_name}"
          end
        end
      end
    end
  end

  desc "Create keyspace and tables for all defined models"
  task :init => %w(keyspace:create migrate)
end
