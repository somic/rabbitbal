#!/usr/bin/env ruby
#
# rabbitbal_rails.rb - Rails backend for Rabbitbal
#
# Usage:
# rabbitbal_rails.rb [-r|--root railsapp_root] \
#                    [-c|--config /path/to/yml] \
#                    [queue1 queue2 ...]
#
# Values of "queue1", "queue2", etc are from :key values under :routing
# For example:
# rabbitbal_rails.rb -r /my/railsapp app.app1 from_cron
#
# require activesupport before json to avoid to_json exceptions
# `to_json': wrong argument type Hash (expected Data) (TypeError)
# http://www.devbaldwin.com/blog/2008/11/18/activesupport-json-to_json-wrong-argument-type/

require 'rubygems'
require 'optparse'
require 'yaml'
require 'activesupport'
$:.unshift(File.join(File.dirname(__FILE__), 'lib'))
require 'rabbitbal_base'

# default values
rails_root = "."
rabbitbal_yaml = "./rabbitbal.yml"

OptionParser.new do |opts|
  opts.banner = "Usage: rabbitbal_rails.rb [options] [queue ...]"
  opts.on("-r", "--root DIR", "Rails app root") { |v| rails_root = v }
  opts.on("-c", "--config PATH",
    "Path to config yaml") { |v| rabbitbal_yaml = v }
end.parse!

$rabbitbal_config = YAML.load_file(rabbitbal_yaml)

queues = { }
if $rabbitbal_config[:routing_type] == :table
  $rabbitbal_config[:routing].each { |h|
    queues[h[:key]] = "request.#{h[:key]}" if ARGV.empty? ||
                                              ARGV.include?(h[:key].to_s)
  }
else
  queues["requests"] = "request.#" 
end
raise "No queues to read from" if queues.empty?

r = Rabbitbal::BackendServer.new(queues,
  Rack::Adapter::Rails.new(:root => rails_root))
r.query_loop { |env| r.rack_adapter.call(env) }

