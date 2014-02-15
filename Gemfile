source 'https://rubygems.org'

gemspec

group :debug do
  gem 'debugger', :platforms => :mri_19
  gem 'byebug', :platforms => :mri_20
end

platform :rbx do
  gem 'racc'
  gem 'rubysl', '~> 2.0'
  gem 'psych'
end
