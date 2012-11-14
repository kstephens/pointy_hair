require "bundler/gem_tasks"
require 'rspec/core/rake_task'
# require 'pp'

RSpec::Core::RakeTask.new(:spec) do | task |
  # pp task.methods.sort - Object.methods
  task.rspec_opts = [ ]
  # task.rspec_opts << '-r ./rspec_config'
  task.rspec_opts << '--color'
  task.rspec_opts << '-f documentation'
end

desc "=> test"
task :default => :test

desc "=> spec"
task :test => :spec


