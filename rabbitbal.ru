
require 'rubygems'
require 'mq'
require 'thin'
require 'json'
require 'sha1'
require 'base64'
require 'yaml'

# from Nanite http://github.com/ezmobius/nanite/tree/master/nanite.ru
#
# you need raggi's patched async version of thin:
# git clone git://github.com/raggi/thin.git
# cd thin
# git branch async
# git checkout async
# git pull origin async_for_rack
# rake install
# thin -R rabbitbal.ru -p 4000 start
#
 
# see rabbitbal.yml for mapping from Rack env hash to AMQP routing keys.
# each request gets a unique ID.
# responses are sent to topic exchange defined in yaml config file,
# routing key "response.#{@identity}.request_unique_id".

RABBITBAL_YAML = 'rabbitbal.yml'

class Request

  ROUTING_KEY_BASE = "request"

  attr_reader :async_callback

  def initialize(env, identity, request_id)
    @env = env
    @hash = Hash.new.merge(@env)
    @hash.delete('rack.errors')
    @async_callback = @hash.delete('async.callback')
    inp = @hash.delete('rack.input')
    @hash['rack.input'] = inp.read
    @hash['rabbitbal.identity'] = identity
    @hash['rabbitbal.request_id'] = request_id
  end

  def to_json
    @hash.to_json
  end

  def key_for_routing_type_topic
    # REQUEST_URI-to-routing_key mapping:
    # / -> request
    # /app/new -> request.app.new
    k = ROUTING_KEY_BASE
    unless @hash['REQUEST_URI'] =~ /^\/+$/
      k += "." + @hash['REQUEST_URI'].split(/\?/)[0].sub(
                    /^\//, '').gsub(/\./, '_').gsub(/\/+/, '.')
    end
    return k
  end

  def key_for_routing_type_table
    # REQUEST_URI-to-routing_key mapping based on :routing element in
    # rabbitbal.yml
    $rabbitbal_config[:routing].each { |h|
      m = h.reject { |k,v| 
        k.is_a?(Symbol) || (v.is_a?(String) ? @hash[k] == v : @hash[k] =~ v) }
      return "#{ROUTING_KEY_BASE}.#{h[:key]}" if m.empty?
    }
    return ROUTING_KEY_BASE
  end

  def key
    return @key if @key
    if $rabbitbal_config[:routing_type] == :table
      @key = key_for_routing_type_table
    else
      @key = key_for_routing_type_topic
    end
    return @key
  end
    
  def request_uri
    @request_uri ||= @hash['REQUEST_URI']
  end

  def request_id
    @hash['rabbitbal.request_id']
  end

end

class RabbitbalApp
  
  AsyncResponse = [-1, {}, []].freeze

  attr_reader :amq, :identity, :config

  def setup_response_queue
    # FIXME
    AMQP.start :host => $rabbitbal_config[:brokers][0][0],
               :port => $rabbitbal_config[:brokers][0][1],
               :user => $rabbitbal_config[:user],
               :pass => $rabbitbal_config[:pass],
               :insist => true
    @amq = MQ.new
    @identity = SHA1.sha1("#{`hostname`} #{$$} #{Time.now} #{rand(0xFFFFF)}")
    @outstanding = Hash.new

    @queue = @amq.queue("read_responses_#{@identity}", :exclusive => true)
    @queue.bind(amq.topic($rabbitbal_config[:exchange]),
                :key => "response.#{@identity}.#").subscribe(
                :no_ack => true) { |info, data|
        request_id = info.routing_key.split(/\./)[-1].to_i
        blk = @outstanding.delete(request_id)
        if blk
          env = JSON.parse(data)
          if env[1]['X-Rabbitbal-Base64']
            body = env.pop[0]
            env << [ Base64.decode64(body) ]
          end
          blk.call(env)
          puts "#{Time.now} [#{request_id}] sent"
        else
          puts "#{Time.now} [#{request_id}] response after timeout or dup" 
        end
    }
    @response_queue_initialized = true
    @request_id = 0
  end

  def rabbitbal_request(req, options={}, &blk)
    @outstanding[req.request_id] = blk
    amq.topic($rabbitbal_config[:exchange]).publish(
      req.to_json, :key => req.key, :immediate => true)

    EM.add_timer($rabbitbal_config[:timeout]) {
      blk = @outstanding.delete(req.request_id)
      if blk
        puts "#{Time.now} [#{req.request_id}] timeout"
        blk.call(nil)
      end
    }
  end
    
  def call(env)
    setup_response_queue unless @response_queue_initialized
    @request_id += 1
    @request_id %= $rabbitbal_config[:max_request_id]
    req = Request.new(env, @identity, @request_id)
    puts "#{Time.now} [#{req.request_id}] received: " +
        "uri=#{req.request_uri} key=#{req.key}"
      
    rabbitbal_request(req) do |response|
      if response
        req.async_callback.call response
      else
        req.async_callback.call [
          500, {'Content-Type' => 'text/html'}, "Request Timeout"]
      end    
    end
    AsyncResponse
  end

end

$rabbitbal_config = YAML.load_file(RABBITBAL_YAML)
unless $rabbitbal_config[:routing].nil? ||
  $rabbitbal_config[:routing].respond_to?(:each)
    raise ":routing is defined in #{RABBITBAL_YAML} but is not an Array"
end

run RabbitbalApp.new

