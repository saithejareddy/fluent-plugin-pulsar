
require "fluent/plugin/output"
require './pulsar_client/PulsarClient'
module Fluent
  module Plugin
    class PulsarOutput < Fluent::Plugin::Output
      Fluent::Plugin.register_input('out_pulsar', self)
      # config Parameters
      desc 'The port of pulsar.'
      config_param :pulsar_port, :integer, default: nil
      desc 'The host of pulsar.'
      config_param :pulsar_host, :string, default: nil
      desc 'The topic of pulsar.'
      config_param :pulsar_topic, :string, default: 'persistent://pulsar/logs/default-topic'
      desc 'The name of producer.'
      config_param :pulsar_producer, :string, default: 'fluentd'
      desc 'The number of messages to send as buffer'
      config_param :num_messages, :int, default: 1
      desc 'The number of retries'
      config_param :num_retry, :int, default: 1
      desc ''
      config_section :parse do
        config_set_default :@type, 'out_pulsar'
      end

      def configure(conf)
        super
      end

      def refresh_brokers(raise_error = true)
        begin
          if @pulsar_host != nil && @pulsar_port != nil
            @producer = Message::PulsarClient.new()
            @producer.connect(@pulsar_host, @pulsar_port)
            log.info "Connected to pulsar brokers successfully"
          end
        rescue Exception => e
          if raise_error
            raise e
          else
            log.error e
          end
        end
      end

      def start
        super
        refresh_brokers
      end

      def proces(tag,es)
          es.each do |time, record|
            log.debug "Publishing records to #{@pulsar_topic}"
            @producer.send(@pulsar_topic,@pulsar_producer,@num_messages,record)
            log.debug "Successfully sent a record to #{@pulsar_topic}"
        end
      end
    end
  end
end
