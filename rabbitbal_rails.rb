#!/usr/bin/env ruby
#

require 'rubygems'
require 'thin'
require 'mq'

# require activesupport before json to avoid to_json exceptions
# `to_json': wrong argument type Hash (expected Data) (TypeError)
# http://www.devbaldwin.com/blog/2008/11/18/activesupport-json-to_json-wrong-argument-type/
require 'activesupport'
require 'json'
require 'base64'

RABBITMQ_SERVER = '127.0.0.1'
RABBITMQ_USER = 'guest'
RABBITMQ_PASS = 'guest'
EXCHANGE = 'rabbitbal'

# FIXME - amqp ack original message
# as of now, Queue#subscribe hard-codes :no_ack => true in
# call to Protocol::Basic::Consume. when this is fixed, switch regular
# subscribe method from the library
MQ::Queue.class_eval do

  # like original subscribe but sets no_ack to false and @ack to true
  def subscribe_and_require_explicit_acks(opts={}, &blk)
    @consumer_tag = "#{name}-#{Kernel.rand(999_999_999_999)}"
    @mq.consumers[@consumer_tag] = self

    raise Error, 'already subscribed to the queue' if @on_msg

    @on_msg = blk
    @ack = true

    @mq.callback{
      @mq.send AMQP::Protocol::Basic::Consume.new({ :queue => @name,
                                              :consumer_tag => @consumer_tag,
                                              :no_ack => !@ack,
                                              :nowait => true }.merge(opts))
    }
    self
  end

  def ack(delivery_tag)
    @mq.callback {
      @mq.send AMQP::Protocol::Basic::Ack.new({
        :delivery_tag => delivery_tag })
    }
  end

  # rewrote receive not to ack automatically for me
  def receive(headers, body)
    return if AMQP.closing
    if cb = (@on_msg || @on_pop)
      cb.call *(cb.arity == 1 ? [body] : [headers, body])
    end
  end

end

# init rails outside of EM event loop because it may take some time
rails_root = File.expand_path(ARGV[0] || ".")
puts "Loading Rails app from #{rails_root}"
rails = Rack::Adapter::Rails.new(:root => rails_root)
puts "Finished loading Rails app"

EM.run {
  AMQP.start :host => RABBITMQ_SERVER,
             :user => RABBITMQ_USER, :pass => RABBITMQ_PASS
  amq = MQ.new

  requests_queue = amq.queue('requests')
  requests_queue.bind(amq.topic(EXCHANGE),
    :key => 'request.#').subscribe_and_require_explicit_acks { |info, data|
      start_time = Time.now
      env = JSON.parse(data)
      inp = env.delete('rack.input')
      env['rack.input'] = StringIO.new(inp)
      rabbitbal_frontend_identity = env.delete('rabbitbal.identity')
      rabbitbal_request_id = env.delete('rabbitbal.request_id')


      resp = rails.call(env)
      rack_data = resp.pop
      resp_data = ""
      rack_data.each { |i| resp_data << i }
      if resp_data.is_binary_data?
        resp << [ Base64.encode64(resp_data) ]
        resp[1]['X-Rabbitbal-Base64'] = 'yes'
      else
        resp << [ resp_data ]
      end

      
      amq.topic(EXCHANGE).publish(resp.to_json, :key =>
        "response.#{rabbitbal_frontend_identity}.#{rabbitbal_request_id}",
        :immediate => true)
      requests_queue.ack(info.delivery_tag)
      puts "#{Time.now} [#{rabbitbal_request_id}] #{env['REQUEST_URI']} " +
           "#{Time.now - start_time}s"
  }

}

