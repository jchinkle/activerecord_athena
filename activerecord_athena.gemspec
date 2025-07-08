Gem::Specification.new do |spec|
  spec.name          = "activerecord_athena"
  spec.version       = "0.1.0"
  spec.authors       = ["Jeremy Hinkle"]
  spec.email         = ["jchinkle@gmail.com"]

  spec.summary       = "ActiveRecord adapter for AWS Athena"
  spec.description   = "An ActiveRecord adapter that enables Rails applications to connect to and query AWS Athena"
  spec.homepage      = "https://github.com/jchinkle/activerecord_athena"
  spec.license       = "MIT"

  spec.files         = Dir.glob("lib/**/*") + ["README.md", "LICENSE"]
  spec.require_paths = ["lib"]

  spec.required_ruby_version = ">= 2.7.0"

  spec.add_dependency "activerecord", ">= 6.0"
  spec.add_dependency "aws-sdk-athena", "~> 1.0"
  spec.add_dependency "aws-sdk-s3", "~> 1.0"

  spec.add_development_dependency "rspec", "~> 3.0"
  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_development_dependency "nokogiri", "~> 1.0"
  spec.add_development_dependency "simplecov", "~> 0.22"
end
