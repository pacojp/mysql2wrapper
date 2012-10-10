# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "mysql2wrapper/version"

Gem::Specification.new do |s|
  s.name        = "mysql2wrapper"
  s.version     = Mysql2wrapper::VERSION
  s.authors     = ["pacojp"]
  s.email       = ["paco.jp@gmail.com"]
  s.homepage    = ""
  s.summary     = %q{oreore mysql2 wrapper class}
  s.description = %q{oreore mysql2 wrapper class}
  s.rubyforge_project = "mysql2wrapper"
  s.add_dependency 'mysql2','0.3.11'

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  # specify any dependencies here; for example:
  # s.add_development_dependency "rspec"
  # s.add_runtime_dependency "rest-client"
end
