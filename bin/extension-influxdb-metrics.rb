#!/usr/bin/env ruby
#
# extension-influxdb-metrics
#
# DESCRIPTION:
#   Robust and highly performant Sensu extension for storing Metrics in InfluxDB
#   Metrics will be buffered until they reach the configured buffer size or maximum age (buffer_size & buffer_max_age)
#   If connectivity failures happen when trying to send metrics it will try a maximum of (buffer_max_try) times and wait (buffer_max_try_delay) before each retry
#
# OUTPUT:
#   metric data
#
# PLATFORMS:
#   Linux
#
# DEPENDENCIES:
#   gem: sensu-plugin
#
# USAGE:
#   0) Create the InfluxDB destination database and retention policies:
#     curl -vXPOST 'http://localhost:8086/query?pretty=true' --data-urlencode "q=CREATE DATABASE sensu_metrics"
#     curl -vXPOST 'http://localhost:8086/query?pretty=true' --data-urlencode "q=CREATE RETENTION POLICY raw ON sensu_metrics DURATION w REPLICATION 1 DEFAULT"
#     curl -vXPOST 'http://localhost:8086/query?pretty=true' --data-urlencode "q=CREATE RETENTION POLICY h5m ON sensu_metrics DURATION 106w REPLICATION 1"
#     curl -vXPOST 'http://localhost:8086/query?pretty=true' --data-urlencode "q=CREATE RETENTION POLICY h1h ON sensu_metrics DURATION 106w REPLICATION 1"
#
#   1) Add the extension-influxdb-metrics.rb to the Sensu extensions folder (/etc/sensu/extensions)
#
#   2) Create the Global InfluxDB configuration and handler for the extention inside the sensu config folder (/etc/sensu/conf.d)
#      echo '{ "influxdb-metrics": { "hostname": "127.0.0.1", "db": "sensu_metrics" } }' >/etc/sensu/conf.d/influxdb_cfg.json
#      echo '{ "handlers": { "metrics": { "type": "set", "handlers": ["influxdb-metrics"] } } }' >/etc/sensu/conf.d/influxdb_handler.json
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
  class InfluxDBMetrics < Handler

    @@extension_name = 'influxdb-metrics'

    def name
      @@extension_name
    end

    def description
      'Historization of Sensu Metrics in InfluxDB'
    end

    def post_init
      influxdb_config = settings[@@extension_name]
      validate_config(influxdb_config)

      hostname               = influxdb_config['hostname']
      port                   = influxdb_config['port'] || 8086
      @USER                  = influxdb_config['user']
      @PASSWD                = influxdb_config['passwd']
      @DB                    = influxdb_config['db']
      @RP                    = influxdb_config['retention_policy']
      @CONSISTENCY           = influxdb_config['consistency'] # all, any, one, quorum
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

      @logger.info("#{@@extension_name}: Successfully initialized: url: #{@URI.to_s}, db: #{@DB}, rp: #{@RP}, cl: #{@CONSISTENCY}, http_compression: #{@HTTP_COMPRESSION}, http_timeout: #{@HTTP_TIMEOUT}:s, buffer_size: #{@BUFFER_SIZE}, buffer_max_age: #{@BUFFER_MAX_AGE}:sec, buffer_max_try: #{@BUFFER_MAX_TRY}, buffer_max_try_delay: #{@BUFFER_MAX_TRY_DELAY}:s")
    end

    def run(event)
      begin
        event = JSON.parse(event)

        # Merge tags from Global, Client, Check and Metric
        tags_global = settings[@@extension_name].fetch(:tags, {})
        tags_client = event['client'].fetch('influxdb', {})['tags'] || Hash.new
        tags_check = event['check'].fetch('influxdb', {})['tags'] || Hash.new
        tags_event = {
          :source => @SOURCE,
          :host => event['client']['name'] || nil,
          :ip => event['client']['address'] || nil
        }
        tags = create_tags(tags_global.merge(tags_client).merge(tags_check).merge(tags_event))

        msg = {
          :parms => {
            :db => event['check'].fetch('influxdb', {})['database'] || @DB,
            :rp => event['check'].fetch('influxdb', {})['retention_policy'] || @RP,
            :consistency => event['check'].fetch('influxdb', {})['consistency'] || @CONSISTENCY,
            :u => event['check'].fetch('influxdb', {})['user'] || @USER,
            :p => event['check'].fetch('influxdb', {})['passwd'] || @PASSWD,
            :precision => event['check'].fetch('influxdb', {})['precision'] || "s" # epoch=[h,m,s,ms,u,ns]
          }
        }

        # Graphite format: <metric> <value> <timestamp>
        metrics = []
        event['check']['output'].split(/\r\n|\n/).each do |line|
          if line.split(/\s+/).size != 3
            @logger.error("Metric is Invalid, skipping metric #{line}")
            next
          end

          key, val, ts = line.split(/\s+/)

          # Remove hostname from measurement and change '.' for '_'
          measurement = key.split('.')[1..-1].join('_')

          if not is_number?(ts)
            @logger.error("Timestamp is Invalid, skipping metric #{line}")
            next
          end

          fields_metric = {
            :duration => event['check']['duration'].to_f || nil,
            :interval => event['check']['interval'] ? "#{event['check']['interval'].to_i}i" : nil,
            :value => val.to_f || nil
          }
          fields = create_tags(fields_metric)[1..-1] || ""

          if fields.to_s.empty?
            @logger.error("Metric field is Invalid, skipping metric #{line}")
            return
          end

          metrics.push("#{measurement}#{tags} #{fields} #{ts.to_i}")
        end

        @logger.debug("#{@@extension_name}: Stored Metrics in buffer (#{@BUFFER.length}/#{@BUFFER_SIZE}) - #{measurement}#{tags} #{fields} #{ts.to_i}")
        @BUFFER.push(msg.merge({:metrics => metrics}))


        if buffer_try_delay? and (buffer_too_old? or buffer_too_big?)
          flush_buffer
        end

      rescue => e
        @logger.error("#{@@extension_name}: Unable to buffer Metrics: #{event} - #{e.message} - #{e.backtrace.to_s}")
      end

      yield("#{@@extension_name}: handler finished", 0)
    end

    def stop
      if !@BUFFER.nil? and @BUFFER.length > 0
        @logger.info("#{@@extension_name}: Flushing Metric buffer before shutdown (#{@BUFFER.length}/#{@BUFFER_SIZE})")
        flush_buffer
      end
    end

    private
    def flush_buffer
      begin
        send_to_inflxudb(@BUFFER)
        @BUFFER = []
        @BUFFER_TRY = 0
        @BUFFER_TRY_SENT = 0
        @BUFFER_FLUSHED = Time.now.to_i

      rescue Exception => e
        @BUFFER_TRY_SENT = Time.now.to_i
        if @BUFFER_TRY >= @BUFFER_MAX_TRY
          @BUFFER = []
          @logger.error("#{@@extension_name}: Maximum retries reached (#{@BUFFER_TRY}/#{@BUFFER_MAX_TRY}), All buffered Metrics have been lost!, #{e.message}")

        else
          @BUFFER_TRY +=1
          @logger.warn("#{@@extension_name}: Writing Metrics to InfluxDB Failed (#{@BUFFER_TRY}/#{@BUFFER_MAX_TRY}), #{e.message}")
        end
      end
    end

    def send_to_inflxudb(events)
      events.each { |e|
        headers = {'Content-Type' => 'text/plain; charset=utf-8'}
        headers['Content-Encoding'] = 'gzip' if @HTTP_COMPRESSION
        headers['User-Agent'] = @@extension_name

        # Prepare write parameter : https://docs.influxdata.com/influxdb/v1.0/write_protocols/write_syntax/
        parms = e[:parms].reject {|k,v| [:u,:p].include?(k) }.to_a.map { |k,v| next if v.to_s.empty?; "#{k}=#{v}" }.compact.join("&") || ""
        url = "#{@URI.request_uri}?#{parms}"

        request = Net::HTTP::Post.new(url, headers)
        request.basic_auth e[:parms][:u], e[:parms][:p] if e[:parms][:u] and e[:parms][:p]

        # Gzip compress
        if @HTTP_COMPRESSION
          compressed = StringIO.new
          gz_writer = Zlib::GzipWriter.new(compressed)
          gz_writer.write(e[:metrics].join("\n"))
          gz_writer.close
          request.body = compressed.string
        else
          request.body = e[:metrics].join("\n")
        end

        @logger.debug("#{@@extension_name}: Writing Metrics: #{request.body} to InfluxDB: #{@URI.to_s}")

        Timeout::timeout(@HTTP_TIMEOUT) do
          ts_s = Time.now.to_i
          response = @HTTP.request(request)
          ts_e = Time.now.to_i
          if response.code.to_i != 204
            @logger.error("#{@@extension_name}: Writing Metrics to InfluxDB: response code = #{response.code}, body = #{response.body}")
            raise "response code = #{response.code}"

          else
            @logger.info("#{@@extension_name}: Sent #{e[:metrics].length} Metrics to InfluxDB in (#{ts_e - ts_s}:s)")
            @logger.debug("#{@@extension_name}: Writing Metrics to InfluxDB: response code = #{response.code}, body = #{response.body}")
          end
        end
      }
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

    # Send Metrics if buffer is to old
    def buffer_too_old?
      buffer_age = Time.now.to_i - @BUFFER_FLUSHED
      buffer_age >= @BUFFER_MAX_AGE
    end

    # Send Metrics if buffer is full
    def buffer_too_big?
      @BUFFER.length >= @BUFFER_SIZE
    end

    def is_number?(input)
      true if Integer(input) rescue false
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
