
require 'rubygems'
require 'mq'
require 'thin'
require 'json'
require 'sha1'
require 'base64'

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
 
# requests are sent to topic exchange defined in EXCHANGE,
# with routing_key which corresponds to URI:
# "/" -> "request"
# "/settings" -> "request.settings"
# "/order/details/45" -> "request.order.details.45"
#
# each request gets a unique ID
#
# responses are sent to topic exchange defined in EXCHANGE,
# routing key "response.#{@identity}.request_unique_id"
#

EXCHANGE = 'rabbitbal'
MAX_REQUEST_ID = 99999999
TIMEOUT = 15
RABBITMQ_SERVER = '127.0.0.1'
RABBITMQ_USER = 'guest'
RABBITMQ_PASS = 'guest'

class Request

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

  def key
    @key ||= "request." + @hash['REQUEST_URI'].split(/\?/)[0].sub(
                    /^\//, '').gsub(/\./, '_').gsub(/\/+/, '.')
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

  attr_reader :amq, :identity

  def setup_response_queue
    AMQP.start :host => RABBITMQ_SERVER,
               :user => RABBITMQ_USER,
               :pass => RABBITMQ_PASS
    @amq = MQ.new
    @identity = SHA1.sha1("#{`hostname`} #{$$} #{Time.now} #{rand(0xFFFFF)}")
    @outstanding = Hash.new
    @amq.queue("read_responses_#{@identity}", :exclusive => true
      ).bind(amq.topic(EXCHANGE), :key => "response.#{@identity}.#"
      ).subscribe(:no_ack => true) { |info, data|
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
    amq.topic(EXCHANGE).publish(req.to_json, :key => req.key,
        :immediate => true)
    EM.add_timer(options[:timeout]) {
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
    @request_id %= MAX_REQUEST_ID
    req = Request.new(env, @identity, @request_id)
    puts "#{Time.now} [#{req.request_id}] received. uri=#{req.request_uri}"
      
    rabbitbal_request(req, :timeout => TIMEOUT) do |response|
      if response
        req.async_callback.call response
      else
        req.async_callback.call [500, {'Content-Type' => 'text/html'}, "Request Timeout"]
      end    
    end
    AsyncResponse
  end

end

run RabbitbalApp.new

