require "activerecord_athena/version"
require "active_record"
require "active_record/connection_adapters/athena_adapter"

module ActiverecordAthena
  class Error < StandardError; end
end