# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'pointy_hair/version'

Gem::Specification.new do |gem|
  gem.name          = "pointy_hair"
  gem.version       = PointyHair::VERSION
  gem.authors       = ["Kurt Stephens"]
  gem.email         = ["kurt@enova.com"]
  gem.description   = %q{TODO: Write a gem description}
  gem.summary       = %q{TODO: Write a gem summary}
  gem.homepage      = ""

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]

  gem.add_development_dependency 'rake', '~> 10.0.3'
  gem.add_development_dependency 'rspec', '~> 2.12.0'
  gem.add_development_dependency 'simplecov', '~> 0.7.0'
end
