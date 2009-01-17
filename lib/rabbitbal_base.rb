#

require 'rubygems'
require 'thin'
require 'mq'
require 'json'
require 'base64'

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

module Rabbitbal
  
  class BackendServer
    
    attr_reader :rack_adapter, :amq

    def initialize(queues, rack_adapter)
      @queues = queues
      @rack_adapter = rack_adapter
    end


    def query_loop(&blk)
      EM.run {
        AMQP.start :host => $rabbitbal_config[:brokers][0][0],
             :port => $rabbitbal_config[:brokers][0][1],
             :user => $rabbitbal_config[:user],
             :pass => $rabbitbal_config[:pass],
             :insist => true
        @amq = MQ.new

        @queues.each_pair do |queue, key|
          q = amq.queue(queue)
          q.bind(amq.topic($rabbitbal_config[:exchange]), :key => key
            ).subscribe_and_require_explicit_acks { |info, data|
              process_query(info, data, &blk)
              q.ack(info.delivery_tag)
            }
        end

        puts "#{Time.now} Ready: #{@queues.inspect}"
      }
    end

    def process_query(info, data, &blk)
      start_time = Time.now
      env = JSON.parse(data)
      inp = env.delete('rack.input')
      env['rack.input'] = StringIO.new(inp)
      rabbitbal_frontend_identity = env.delete('rabbitbal.identity')
      rabbitbal_request_id = env.delete('rabbitbal.request_id')

      resp = blk.call(env)

      rack_data = resp.pop
      resp_data = ""
      rack_data.each { |i| resp_data << i }
      if resp_data.is_binary_data?
        resp << [ Base64.encode64(resp_data) ]
        resp[1]['X-Rabbitbal-Base64'] = 'yes'
      else
        resp << [ resp_data ]
      end

      amq.topic($rabbitbal_config[:exchange]).publish(resp.to_json, :key =>
        "response.#{rabbitbal_frontend_identity}.#{rabbitbal_request_id}",
        :immediate => true)
      puts "#{Time.now} [#{rabbitbal_request_id}] #{env['REQUEST_URI']} " +
           "#{Time.now - start_time}s"
    end

  end
end

