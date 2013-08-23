require 'bundler'

if ENV['CI']
  Bundler.require(:default, :test)
else
  Bundler.require(:default, :test, :debug)
end
