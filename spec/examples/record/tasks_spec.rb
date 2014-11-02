# -*- encoding : utf-8 -*-
require File.expand_path('../../spec_helper', __FILE__)
require 'rake'
require File.expand_path('../../../../lib/cequel/record/tasks', __FILE__)

describe 'cequel:migrate rake task' do
  before :all do
    Rake::Task.define_task(:environment)
  end

  describe 'when run with directory-nested models' do
    let :run_rake_task do
      Rake::Task["cequel:migrate"].reenable
      Rake.application.invoke_task "cequel:migrate"
    end

    it "should skip Model::CONSTANT declarations" do
      run_rake_task
    end
  end
end
