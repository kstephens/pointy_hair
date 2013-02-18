unless RUBY_VERSION =~ /^1\.8/
  require 'simplecov'
  SimpleCov.start do
    add_filter "spec/"
  end
end

require 'pointy_hair'

require 'fileutils'
