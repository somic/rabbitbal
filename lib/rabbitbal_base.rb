#

require 'rubygems'
require 'thin'
require 'mq'
require 'json'
require 'base64'

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
            ).subscribe(:ack => true) { |headers, data|
              process_query(headers, data, &blk)
              headers.ack
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

