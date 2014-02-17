# -*- encoding : utf-8 -*-
require 'bundler'

if ENV['CI']
  Bundler.require(:default, :development)
else
  Bundler.require(:default, :development, :debug)
end
