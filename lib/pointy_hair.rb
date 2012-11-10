# -- encoding : utf-8 --
require "pointy_hair/version"

module PointyHair
end

if RUBY_VERSION =~ /^1\.8/
  require 'pointy_hair/ruby_18'
end
require 'pointy_hair/work'
require 'pointy_hair/worker'
require 'pointy_hair/manager'

