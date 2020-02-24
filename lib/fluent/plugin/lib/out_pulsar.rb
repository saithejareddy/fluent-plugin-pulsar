
require "fluent/plugin/output"
require 'pulsar_client/PulsarClient'
module Fluent
  module Plugin
    class PulsarOutput < Fluent::Plugin::Output
      Fluent::Plugin.register_input('out_pulsar', self)
      # config Parameters
      desc 'The port of pulsar.'
      config_param :pulsar_port, :integer, default: 6650
      desc 'The host of pulsar.'
      config_param :pulsar_host, :string, default: 'pulsar-proxy.pulsar'
      desc 'The topic of pulsar.'
      config_param :pulsar_topic, :string, default: 'persistent://pulsar/logs/default-topic'
      desc 'The name of producer.'
      config_param :pulsar_producer, :string, default: 'fluentd'
      desc 'The number of messages to send as buffer'
      config_param :num_messages, :int, default: 1
      desc 'The number of retries'
      config_param :num_retry, :int, default: 1
      config_section :parse do
        config_set_default :@type, 'out_pulsar'
      end
      def configure(conf)
        super

      end
      def start
        super
        client = Message::PulsarClient.new()
        client.connect(@pulsar_host, @pulsar_port)
        log.info "Connected to pulsar brokers successfully"
      end
      def process(tag,es)
        es.each do |time,record|
          log.debug "Producing records to #{@pulsar_topic}"
          send_to_pulsar(record)
        end
      end
      def send_to_pulsar(record)
        retry_count = 0
        if client.send(@pulsar_topic, @pulsar_producer, @num_messages, record) == true
          log.info "Successfully sent a record"
        else
          log.warn "Failed to send, retrying.."
          retry_count += 1
          if (retry_count <= @num_retry)
            send_to_pulsar(record)
          else
            log.error "retry limit exceeded, dropping record.."
          end
        end
      end
    end
  end
end
