#!/usr/bin/env ruby
#
# extension-influxdb-events
#
# DESCRIPTION:
#   Robust and highly performant Sensu extension for storing Events in InfluxDB
#   Events will be buffered until they reach the configured buffer size or maximum age (buffer_size & buffer_max_age)
#   If connectivity failures happen when trying to send events it will try a maximum of (buffer_max_try) times and wait (buffer_max_try_delay) before each retry
#
# OUTPUT:
#   event data
#
# PLATFORMS:
#   Linux
#
# DEPENDENCIES:
#   gem: sensu-plugin
#
# USAGE:
#   0) Create the InfluxDB destination database and retention policies:
#     curl -vXPOST 'http://localhost:8086/query?pretty=true' --data-urlencode "q=CREATE DATABASE sensu_events"
#     curl -vXPOST 'http://localhost:8086/query?pretty=true' --data-urlencode "q=CREATE RETENTION POLICY raw ON sensu_events DURATION 8w REPLICATION 1 DEFAULT"
#     curl -vXPOST 'http://localhost:8086/query?pretty=true' --data-urlencode "q=CREATE RETENTION POLICY h5m ON sensu_events DURATION 106w REPLICATION 1"
#     curl -vXPOST 'http://localhost:8086/query?pretty=true' --data-urlencode "q=CREATE RETENTION POLICY h1h ON sensu_events DURATION 106w REPLICATION 1"
#
#   1) Add the extension-influxdb-events.rb to the Sensu extensions folder (/etc/sensu/extensions)
#
#   2) Create the Global InfluxDB configuration and handler for the extention inside the sensu config folder (/etc/sensu/conf.d)
#      echo '{ "influxdb-events": { "hostname": "127.0.0.1", "db": "sensu_events" } }' >/etc/sensu/conf.d/influxdb_cfg.json
#      echo '{ "handlers": { "events": { "type": "set", "handlers": ["influxdb-events"] } } }' >/etc/sensu/conf.d/influxdb_handler.json
#
#
# NOTES:
#
# LICENSE:
#   Copyright 2016 Sebastian YEPES <syepes@gmail.com>
#   Released under the same terms as Sensu (the MIT license); see LICENSE for details.
#

require 'net/http'
require 'timeout'
require 'json'

module Sensu::Extension
  class InfluxDBEvents < Handler

    @@extension_name = 'influxdb-events'

    def name
      @@extension_name
    end

    def description
      'Historization of Sensu Events in InfluxDB'
    end

    def post_init
      influxdb_config = settings[@@extension_name]
      validate_config(influxdb_config)

      hostname               = influxdb_config['hostname']
      port                   = influxdb_config['port'] || 8086
      user                   = influxdb_config['user']
      passwd                 = influxdb_config['passwd']
      db                     = influxdb_config['db']
      rp                     = influxdb_config['retention_policy']
      consistency            = influxdb_config['consistency'] # all, any, one, quorum
      ssl                    = influxdb_config['ssl'] || false
      ssl_cert               = influxdb_config['ssl_cert']
      protocol               = if ssl then 'https' else 'http' end
      @SOURCE                = influxdb_config['source'] || 'sensu'
      @HTTP_COMPRESSION      = influxdb_config['http_compression'] || true
      @HTTP_TIMEOUT          = influxdb_config['http_timeout'] || 15 # seconds
      @BUFFER_SIZE           = influxdb_config['buffer_size'] || 5125
      @BUFFER_MAX_AGE        = influxdb_config['buffer_max_age'] || 300 # seconds
      @BUFFER_MAX_TRY        = influxdb_config['buffer_max_try'] || 6
      @BUFFER_MAX_TRY_DELAY  = influxdb_config['buffer_max_try_delay'] || 120 # seconds

      @PARMS = {
        :db => db,
        :rp => rp,
        :consistency => consistency,
        :u => user,
        :p => passwd,
        :precision => "s"
      }

      @URI = URI("#{protocol}://#{hostname}:#{port}/write")
      @HTTP = Net::HTTP::new(@URI.host, @URI.port)

      if ssl
        @HTTP.ssl_version = :TLSv1
        @HTTP.use_ssl = true
        @HTTP.verify_mode = OpenSSL::SSL::VERIFY_PEER
        @HTTP.ca_file = ssl_cert
      end

      @BUFFER = []
      @BUFFER_TRY = 0
      @BUFFER_TRY_SENT = 0
      @BUFFER_FLUSHED = Time.now.to_i

      @logger.info("#{@@extension_name}: Successfully initialized: url: #{@URI.to_s}, db: #{@PARMS[:db]}, rp: #{@PARMS[:db]}, cl: #{@PARMS[:consistency]}, http_compression: #{@HTTP_COMPRESSION}, http_timeout: #{@HTTP_TIMEOUT}:s, buffer_size: #{@BUFFER_SIZE}, buffer_max_age: #{@BUFFER_MAX_AGE}:sec, buffer_max_try: #{@BUFFER_MAX_TRY}, buffer_max_try_delay: #{@BUFFER_MAX_TRY_DELAY}:s")
    end

    def run(event)
      begin
        event = JSON.parse(event)

        # Remove hostname from measurement and change '.' for '_'
        measurement = event['check']['name'].tr('.','_').tr(' ','_')

        # Merge tags from Global, Client, Check and Event
        tags_global = settings[@@extension_name].fetch(:tags, {})
        tags_client = event['client'].fetch('influxdb', {})['tags'] || Hash.new
        tags_check = event['check'].fetch('influxdb', {})['tags'] || Hash.new
        tags_event = {
          :source => @SOURCE || nil,
          :client => event['client']['name'] || nil
        }
        tags = create_tags(tags_global.merge(tags_client).merge(tags_check).merge(tags_event))

        # Event fields
        fields_event = {
          :type => event['check']['type'] ? "\"#{event['check']['type']}\"" : nil,
          :client_ip => event['client']['address'] ? "\"#{event['client']['address']}\"" : nil,
          :client_ver => event['client']['version'] ? "\"#{event['client']['version']}\"" : nil,
          :action => event['action'] ? "\"#{event['action']}\"" : nil,
          :status => event_status(event['check']['status']) ? "\"#{event_status(event['check']['status'])}\"" : nil,
          :interval => event['check']['interval'] ? "#{event['check']['interval'].to_i}i" : nil,
          :occurrences => event['occurrences'] ? "#{event['occurrences']}i" : nil,
          :issued => event['check']['issued'] ? "#{event['check']['issued']}i" : nil,
          :executed => event['check']['executed'] ? "#{event['check']['executed']}i" : nil,
          :history => event['check']['history'].any? ? "\"#{event['check']['history'].join(',')}\"" : nil
        }
        fields = create_tags(fields_event)[1..-1] || ""

        if fields.to_s.empty?
          @logger.error("Event is Invalid, skipping event #{event}")
          return
        end

        ts = event['timestamp']
        if not is_number?(ts)
          @logger.error("Timestamp is Invalid, skipping event #{event}")
          return
        end

        @BUFFER.push("#{measurement}#{tags} #{fields} #{ts.to_i}")
        @logger.debug("#{@@extension_name}: Stored Event in buffer (#{@BUFFER.length}/#{@BUFFER_SIZE}) - #{measurement}#{tags} #{fields} #{ts.to_i}")

        if buffer_try_delay? and (buffer_too_old? or buffer_too_big?)
          flush_buffer
        end

      rescue => e
        @logger.error("#{@@extension_name}: Unable to buffer Event: #{event} - #{e.message} - #{e.backtrace.to_s}")
      end

      yield("#{@@extension_name}: handler finished", 0)
    end

    def stop
      if !@BUFFER.nil? and @BUFFER.length > 0
        @logger.info("#{@@extension_name}: Flushing Event buffer before shutdown (#{@BUFFER.length}/#{@BUFFER_SIZE})")
        flush_buffer
      end
    end

    private
    def flush_buffer
      begin
        send_to_influxdb(@BUFFER)
        @BUFFER = []
        @BUFFER_TRY = 0
        @BUFFER_TRY_SENT = 0
        @BUFFER_FLUSHED = Time.now.to_i

      rescue Exception => e
        @BUFFER_TRY_SENT = Time.now.to_i
        if @BUFFER_TRY >= @BUFFER_MAX_TRY
          @BUFFER = []
          @logger.error("#{@@extension_name}: Maximum retries reached (#{@BUFFER_TRY}/#{@BUFFER_MAX_TRY}), All buffered Events have been lost!, #{e.message}")

        else
          @BUFFER_TRY +=1
          @logger.warn("#{@@extension_name}: Writing Events to InfluxDB Failed (#{@BUFFER_TRY}/#{@BUFFER_MAX_TRY}), #{e.message}")
        end
      end
    end

    def send_to_influxdb(events)
      headers = {'Content-Type' => 'text/plain; charset=utf-8'}
      headers['Content-Encoding'] = 'gzip' if @HTTP_COMPRESSION
      headers['User-Agent'] = @@extension_name

      # Prepare write parameter : https://docs.influxdata.com/influxdb/v1.0/write_protocols/write_syntax/
      parms = @PARMS.reject {|k,v| [:u,:p].include?(k) }.to_a.map { |k,v| next if v.to_s.empty?; "#{k}=#{v}" }.compact.join("&") || ""
      url = "#{@URI.request_uri}?#{parms}"

      request = Net::HTTP::Post.new(url, headers)
      request.basic_auth @PARMS[:u], @PARMS[:p] if @PARMS[:u] and @PARMS[:p]

      # Gzip compress
      if @HTTP_COMPRESSION
        compressed = StringIO.new
        gz_writer = Zlib::GzipWriter.new(compressed)
        gz_writer.write(events.join("\n"))
        gz_writer.close
        request.body = compressed.string
      else
        request.body = events.join("\n")
      end

      @logger.debug("#{@@extension_name}: Writing Events: #{request.body} to InfluxDB: #{@URI.to_s}")

      Timeout::timeout(@HTTP_TIMEOUT) do
        ts_s = Time.now.to_i
        response = @HTTP.request(request)
        ts_e = Time.now.to_i
        if response.code.to_i != 204
          @logger.error("#{@@extension_name}: Writing Events to InfluxDB: response code = #{response.code}, body = #{response.body}")
          raise "response code = #{response.code}"

        else
          @logger.info("#{@@extension_name}: Sent #{events.length} Events to InfluxDB in (#{ts_e - ts_s}:s)")
          @logger.debug("#{@@extension_name}: Writing Events to InfluxDB: response code = #{response.code}, body = #{response.body}")
        end
      end
    end


    # Establish a delay between retrie failures
    def buffer_try_delay?
      seconds = (Time.now.to_i - @BUFFER_TRY_SENT)
      if seconds < @BUFFER_MAX_TRY_DELAY
        @logger.warn("#{@@extension_name}: Waiting for (#{seconds}/#{@BUFFER_MAX_TRY_DELAY}) seconds before next retry") if ( ((@BUFFER_MAX_TRY_DELAY - seconds) % @BUFFER_MAX_TRY+1) == 0 )
        false

      else
        true
      end
    end

    # Cretae tags optimized for InfluxDB
    def create_tags(tags)
      begin
        # Sorting tags alphabetically in order to increase influxdb performance
        sorted_tags = Hash[tags.map{ |k, v| [k.to_sym, v] }.sort]

        tag_string = ""
        sorted_tags.each do |tag, value|
          next if value.to_s.empty? # skips tags without values
          tag_string += ",#{tag}=#{value}"
        end

        @logger.debug("#{@@extension_name}: Created tags: #{tag_string}")
        tag_string
      rescue => e
        @logger.error("#{@@extension_name}: Unable to create tag string from #{tags} - #{e.backtrace.to_s}")
        ""
      end
    end

    # Send Events if buffer is to old
    def buffer_too_old?
      buffer_age = Time.now.to_i - @BUFFER_FLUSHED
      buffer_age >= @BUFFER_MAX_AGE
    end

    # Send Events if buffer is full
    def buffer_too_big?
      @BUFFER.length >= @BUFFER_SIZE
    end

    def is_number?(input)
      true if Integer(input) rescue false
    end

    def event_status(status)
      case status
      when 0
        'Ok'
      when 1
        'Warning'
      when 2
        'Critical'
      else
        'Unknown'
      end
    end

    def validate_config(config)
      if config.nil?
        raise ArgumentError, "No configuration for #{@@extension_name} provided. exiting..."
      end

      ["hostname", "db"].each do |required_setting|
        if config[required_setting].nil?
          raise ArgumentError, "Required setting #{required_setting} not provided to extension. this should be provided as json element with key '#{@@extension_name}'. exiting..."
        end
      end
    end

  end
end
