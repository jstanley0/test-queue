module TestQueue
  class Iterator
    attr_reader :stats, :sock

    def initialize(sock, suites, filter=nil, runner: nil)
      @start = Time.now
      @waiting_time = 0
      @done = false
      @stats = {}
      @procline = $0
      @sock = sock
      @suites = suites
      @filter = filter
      if @sock =~ /^(.+):(\d+)$/
        @tcp_address = $1
        @tcp_port = $2.to_i
      end
      @runner = runner
    end

    def query(payload)
      query_start = Time.now
      return if @done
      client = connect_to_master(payload)
      _r, _w, e = IO.select([client], nil, [client], nil)
      return if !e.empty?

      if data = client.read(65536)
        client.close
        item = Marshal.load(data)
        return if item.nil? || item.empty?
        item
      end
    rescue Errno::ENOENT, Errno::ECONNRESET, Errno::ECONNREFUSED, Errno::ETIMEDOUT, Errno::EPIPE
      @done = true
      puts "rescued #{$!} for #{payload.lines.first}"
    ensure
      @waiting_time += Time.now - query_start
    end

    def each
      fail "already used this iterator. previous caller: #@done" if @done

      while item = query("POP\n")
        suite = @suites[item]

        $0 = "#{@procline} - #{suite.respond_to?(:description) ? suite.description : suite}"
        capture_timing(suite) do
          if @filter
            @filter.call(suite){ yield suite }
          else
            yield suite
          end
        end
      end
    ensure
      @done = caller.first
      puts "total time: #{Time.now - @start}"
      puts "waiting time: #{@waiting_time} seconds"
      File.open("/tmp/test_queue_worker_#{$$}_stats", "wb") do |f|
        f.write Marshal.dump(@stats)
      end
    end

    def capture_timing(item)
      start = Time.now
      result = yield
      @stats[item.to_s] = Time.now - start
      result
    end

    def connect_to_master(cmd)
      sock =
        if @tcp_address
          Socket.tcp(@tcp_address, @tcp_port, connect_timeout: 30)
        else
          UNIXSocket.new(@sock)
        end
      sock.write(cmd)
      sock
    end

    include Enumerable

    def empty?
      false
    end
  end
end
