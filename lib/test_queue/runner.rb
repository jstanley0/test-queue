require 'socket'
require 'fileutils'
require 'securerandom'

module TestQueue
  class Worker
    attr_accessor :pid, :status, :output, :stats, :num, :host
    attr_accessor :start_time, :end_time
    attr_accessor :summary, :failure_output

    def initialize(pid, num)
      @pid = pid
      @num = num
      @start_time = Time.now
      @output = ''
      @stats = {}
    end

    def lines
      @output.split("\n")
    end
  end

  class Runner
    attr_accessor :concurrency, :exit_when_done

    def initialize(queue, concurrency=nil, socket=nil, relay=nil)
      raise ArgumentError, 'array(-ish) required' unless Array === queue || queue.respond_to?(:shift)

      if forced = ENV['TEST_QUEUE_FORCE']
        forced = forced.split(/\s*,\s*/)
        whitelist = Set.new(forced)
        queue = queue.select{ |s| whitelist.include?(s.to_s) }
        queue.sort_by!{ |s| forced.index(s.to_s) }
      end

      @procline = $0
      prepare_queue(queue)

      @workers = {}
      @completed = []

      @concurrency =
        concurrency ||
        (ENV['TEST_QUEUE_WORKERS'] && ENV['TEST_QUEUE_WORKERS'].to_i) ||
        if File.exists?('/proc/cpuinfo')
          File.read('/proc/cpuinfo').split("\n").grep(/processor/).size
        elsif RUBY_PLATFORM =~ /darwin/
          `/usr/sbin/sysctl -n hw.activecpu`.to_i
        else
          2
        end

      @slave_connection_timeout =
        (ENV['TEST_QUEUE_RELAY_TIMEOUT'] && ENV['TEST_QUEUE_RELAY_TIMEOUT'].to_i) ||
        30

      @run_token = ENV['TEST_QUEUE_RELAY_TOKEN'] || SecureRandom.hex(8)

      @socket =
        socket ||
        ENV['TEST_QUEUE_SOCKET'] ||
        "/tmp/test_queue_#{$$}_#{object_id}.sock"

      @relay =
        relay ||
        ENV['TEST_QUEUE_RELAY']

      @slave_message = ENV["TEST_QUEUE_SLAVE_MESSAGE"] if ENV.has_key?("TEST_QUEUE_SLAVE_MESSAGE")

      if @relay == @socket
        STDERR.puts "*** Detected TEST_QUEUE_RELAY == TEST_QUEUE_SOCKET. Disabling relay mode."
        @relay = nil
      elsif @relay
        @queue = []
      end

      @exit_when_done = true
    end

    def prepare_queue(queue)
      @queue = queue
      @suites = @queue.inject(Hash.new){ |hash, suite| hash.update suite.to_s => suite }
    end

    def stats
      @stats ||=
        if File.exists?(file = stats_file)
          Marshal.load(IO.binread(file)) || {}
        else
          {}
        end
    end

    def new_stats
      @new_stats ||= {}
    end

    # Run the tests.
    #
    # If exit_when_done is true, exit! will be called before this method
    # completes. If exit_when_done is false, this method will return an Integer
    # number of failures.
    def execute
      $stdout.sync = $stderr.sync = true
      @start_time = Time.now

      @concurrency >= 0 ?
        execute_parallel :
        execute_sequential

      exitstatus = summarize_internal
      if exit_when_done
        exit! exitstatus
      else
        exitstatus
      end
    end

    def summarize_internal
      puts
      puts "==> Summary (#{@completed.size} workers in %.4fs)" % (Time.now-@start_time)
      puts

      @failures = ''
      @completed.each do |worker|
        summarize_worker(worker)
        @failures << worker.failure_output if worker.failure_output

        puts "    [%2d] %60s      %4d suites in %.4fs      (pid %d exit %d%s)" % [
          worker.num,
          worker.summary,
          worker.stats.size,
          worker.end_time - worker.start_time,
          worker.pid,
          worker.status.exitstatus || 1,
          worker.host && " on #{worker.host.split('.').first}"
        ] rescue puts("error outputting worker: #{worker.inspect}")
      end

      unless @failures.empty?
        puts
        puts "==> Failures"
        puts
        puts @failures
      end

      puts

      save_stats
      summarize

      estatus = 0
      estatus = 1 if @completed.any? { |worker| worker.status.exitstatus != 0 }
      estatus = 1 if @timed_out
      estatus
    end

    def save_stats
      result = new_stats
      if result.present?
        result = stats.merge(result) unless ENV["TEST_QUEUE_REPLACE_STATS"]
        File.open(stats_file, 'wb') do |f|
          f.write Marshal.dump(result)
        end
      end
    end

    def summarize
    end

    def stats_file
      ENV['TEST_QUEUE_STATS'] ||
      '.test_queue_stats'
    end

    def execute_sequential
      run_worker(@queue)
    end

    def execute_parallel
      start_master
      prepare(@concurrency)
      @prepared_time = Time.now
      start_relay if relay?
      spawn_workers
      distribute_queue
    ensure
      puts "got #{$!} while running execute_parallel\n" + $!.backtrace[0..5].join("\n") if $!
      stop_master

      kill_workers
    end

    def start_master
      if !relay?
        if @socket =~ /^(?:(.+):)?(\d+)$/
          address = $1 || '0.0.0.0'
          port = $2.to_i
          @socket = "#$1:#$2"
          @server = TCPServer.new(address, port)
        else
          FileUtils.rm(@socket) if File.exists?(@socket)
          @server = UNIXServer.new(@socket)
        end
      end

      desc = "test-queue master (#{relay?? "relaying to #{@relay}" : @socket})"
      puts "Starting #{desc}"
      $0 = "#{desc} - #{@procline}"
    end

    def start_relay
      return unless relay?

      sock = connect_to_relay
      message = @slave_message ? " #{@slave_message}" : ""
      message.gsub!(/(\r|\n)/, "") # Our "protocol" is newline-separated
      sock.puts("SLAVE #{@concurrency} #{Socket.gethostname} #{@run_token}#{message}")
      response = sock.gets.strip
      unless response == "OK"
        STDERR.puts "*** Got non-OK response from master: #{response}"
        sock.close
        exit! 1
      end
      sock.close
    rescue Errno::ECONNREFUSED
      STDERR.puts "*** Unable to connect to relay #{@relay}. Aborting.."
      exit! 1
    end

    def stop_master
      return if relay?

      FileUtils.rm_f(@socket) if @socket && @server.is_a?(UNIXServer)
      @server.close rescue nil if @server
      @socket = @server = nil
    end

    def spawn_workers
      @concurrency.times do |i|
        num = i+1

        pid = fork do
          @server.close if @server

          iterator = Iterator.new(relay?? @relay : @socket, @suites, method(:around_filter), runner: self)
          after_fork_internal(num, iterator)
          ret = run_worker(iterator) || 0
          cleanup_worker
          Kernel.exit! ret
        end

        @workers[pid] = Worker.new(pid, num)
      end
    end

    def after_fork_internal(num, iterator)
      srand

      output = File.open("/tmp/test_queue_worker_#{$$}_output", 'w')

      $stdout.reopen(output)
      $stderr.reopen($stdout)
      $stdout.sync = $stderr.sync = true

      $0 = "test-queue worker [#{num}]"
      puts
      puts "==> Starting #$0 (#{Process.pid} on #{Socket.gethostname}) - iterating over #{iterator.sock}"
      puts

      after_fork(num)
    end

    # Run in the master before the fork. Used to create
    # concurrency copies of any databases required by the
    # test workers.
    def prepare(concurrency)
    end

    def around_filter(suite)
      yield
    end

    # Prepare a worker for executing jobs after a fork.
    def after_fork(num)
    end

    # Entry point for internal runner implementations. The iterator will yield
    # jobs from the shared queue on the master.
    #
    # Returns an Integer number of failures.
    def run_worker(iterator)
      iterator.each do |item|
        puts "  #{item.inspect}"
      end

      return 0 # exit status
    end

    def cleanup_worker
    end

    def summarize_worker(worker)
      worker.summary = ''
      worker.failure_output = ''
    end

    def reap_worker(blocking=true)
      if pid = Process.waitpid(-1, blocking ? 0 : Process::WNOHANG) and worker = @workers.delete(pid)
        worker.status = $?
        worker.end_time = Time.now

        if File.exists?(file = "/tmp/test_queue_worker_#{pid}_output")
          worker.output = IO.binread(file)
          FileUtils.rm(file)
        end

        if File.exists?(file = "/tmp/test_queue_worker_#{pid}_stats")
          worker.stats = Marshal.load(IO.binread(file))
          FileUtils.rm(file)
        end

        relay_to_master(worker) if relay?
        worker_completed(worker)
      end
    end

    def worker_completed(worker)
      return if @aborting
      @completed << worker
      puts worker.output if ENV['TEST_QUEUE_VERBOSE'] || worker.status.exitstatus != 0
    end

    def queue_empty?
      @queue.empty?
    end

    def bad_worker_timeout
      (ENV["TEST_QUEUE_BAD_WORKER_TIMEOUT"] || 120).to_i
    end

    def queue_size
      @queue.size
    end

    def record_bad_workers
      sad_workers = @workers.values
      @remote_workers.each do |host, count|
        sad_workers.concat(count.times.map { |i| Worker.new(0, 0) })
      end

      if sad_workers.present?
        puts "No remaining workers have checked in for #{bad_worker_timeout} seconds, aborting."
      else
        puts "All workers have completed, but there are still items in the queue, aborting."
      end
      puts "Either you have some broken code, or a bad node :("
      puts
      puts "Queue size: #{queue_size}"
      puts "Local workers: #{@workers.size}" if @workers.any?
      if @remote_workers.any?
        puts "Remote workers:"
        @remote_workers.each do |host, count|
          puts "  #{host}: #{count}"
        end
      end

      @timed_out = true
      @workers = {}
      fake_status = Struct.new(:exitstatus).new(1)
      sad_workers.each do |worker|
        worker.status = fake_status
        worker.end_time = Time.now
      end
      @completed.concat sad_workers
    end

    def distribute_queue
      return if relay?
      @remote_workers = {}
      last_command = nil

      puts "Distributing queue" if ENV['TEST_QUEUE_VERBOSE']
      until queue_empty? && @remote_workers.empty?
        queue_status(@start_time, @queue.size, @workers.size, @remote_workers)

        if IO.select([@server], nil, nil, 0.1).nil?
          reap_worker(false) if @workers.any? # check for worker deaths
          if last_command && Time.now > last_command + bad_worker_timeout
            record_bad_workers
            break
          end
        else
          last_command = Time.now
          sock = @server.accept
          cmd = sock.gets.strip
          handle_command(cmd, sock)
          sock.close
        end
      end
    ensure
      puts "got #{$!} while running distribute_queue\n" + $!.backtrace[0..5].join("\n") if $!
      stop_master

      until @workers.empty?
        reap_worker
      end
    end

    def log_last_assignment(obj, sock)
      addr = sock.peeraddr(false)
      worker = "#{addr[2]}:#{addr[1]}" # IP:port
      #@assignments ||= {}
      #@assignments[worker] = obj
      STDERR.puts "*** assigned #{obj} to #{worker}"
    end

    def handle_command(cmd, sock)
      case cmd
      when /^POP/
        # If we have a slave from a different test run, don't respond, and it will consider the test run done.
        if obj = @queue.shift
          data = Marshal.dump(obj.to_s)
          sock.write(data)
          log_last_assignment(obj, sock)
        end
      when /^SLAVE (\d+) ([\w\.-]+) (\w+)(?: (.+))?/
        num = $1.to_i
        slave = $2
        run_token = $3
        slave_message = $4
        if run_token == @run_token
          # If we have a slave from a different test run, don't respond, and it will consider the test run done.
          sock.write("OK\n")
          @remote_workers[slave] = num
        else
          STDERR.puts "*** Worker from run #{run_token} connected to master for run #{@run_token}; ignoring."
          sock.write("WRONG RUN\n")
        end
        message = "*** #{num} workers connected from #{slave} after #{Time.now-@start_time}s"
        message << " " + slave_message if slave_message
        STDERR.puts message
      when /^WORKER (\d+)/
        data = sock.read($1.to_i)
        worker = Marshal.load(data)
        worker_completed(worker)
        @remote_workers[worker.host] -= 1
        @remote_workers.delete(worker.host) if @remote_workers[worker.host] == 0
        sock.write("OK\n")
      else
        STDERR.puts("Ignoring unrecognized command: \"#{cmd}\"")
      end
    end

    def relay?
      !!@relay
    end

    def connect_to_relay
      sock = nil
      start = Time.now
      puts "Attempting to connect for #{@slave_connection_timeout}s..."
      while sock.nil?
        begin
          sock = Socket.tcp(*@relay.split(':'), connect_timeout: 30)
        rescue Errno::ECONNREFUSED, Errno::ETIMEDOUT
          raise if Time.now - start > @slave_connection_timeout
          puts "Master not yet available, sleeping..."
          sleep 0.5
        end
      end
      sock
    end

    def relay_to_master(worker)
      worker.host = Socket.gethostname
      data = Marshal.dump(worker)

      puts "WORKER #{worker.num} (pid: #{worker.pid}, #{Time.now})"
      sock = connect_to_relay
      sock.puts("WORKER #{data.bytesize}")
      sock.write(data)
      response = sock.gets.strip
      unless response == "OK"
        STDERR.puts "*** Got non-OK response from master: #{response}"
      else
        puts "OK"
      end
    ensure
      puts "got #{$!} while relaying\n" + $!.backtrace[0..5].join("\n") if $!
      sock.close if sock
    end

    def kill_workers
      @workers.each do |pid, worker|
        Process.kill 'KILL', pid
      end

      until @workers.empty?
        reap_worker
      end
    end

    # Stop the test run immediately.
    #
    # message - String message to print to the console when exiting.
    #
    # Doesn't return.
    def abort(message)
      @aborting = true
      kill_workers
      Kernel::abort("Aborting: #{message}")
    end

    # Subclasses can override to monitor the status of the queue.
    #
    # For example, you may want to record metrics about how quickly remote
    # workers connect, or abort the build if not enough connect.
    #
    # This method is called very frequently during the test run, so don't do
    # anything expensive/blocking.
    #
    # This method is not called on remote masters when using remote workers,
    # only on the central master.
    #
    # start_time          - Time when the test run began
    # queue_size          - Integer number of suites left in the queue
    # local_worker_count  - Integer number of active local workers
    # remote_worker_count - Integer number of active remote workers
    #
    # Returns nothing.
    def queue_status(start_time, queue_size, local_worker_count, remote_worker_count)
    end
  end
end
