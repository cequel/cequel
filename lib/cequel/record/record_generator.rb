# -*- encoding : utf-8 -*-
module Cequel
  module Record
    #
    # Rails generator to create a record class
    #
    # @since 1.0.0
    #
    class RecordGenerator < Rails::Generators::NamedBase
      namespace 'cequel'
      source_root File.expand_path('../../../../templates', __FILE__)
      argument :attributes, type: :array, default: [],
                            banner: 'field:type[:index] field:type[:index]'

      #
      # Create a Record implementation
      #
      def create_record
        template 'record.rb',
                 File.join('app/models', class_path, "#{file_name}.rb")
      end
    end
  end
end

