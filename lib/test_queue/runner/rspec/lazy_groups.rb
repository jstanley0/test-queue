require_relative "group_queue"
require 'zlib'
require 'socket'
require 'base64'

class TestQueue::Runner::RSpec
  module LazyGroups
    class << self
      attr_writer :loader

      def loader
        @loader ||= BackgroundLoader
      end
    end

    module Runner
      def initialize(*)
        Queue.group_loaded = method(:add_group_queue)
        BackgroundLoaderProxy.stats = stats
        super no_sort: true
      end

      def after_fork_internal(*)
        super
        LazyGroups.loader = GroupLoader
      end

      def prepare_queue(*)
        @queue = Queue
        @suites = GroupLoader
      end

      def normalize_scope(scope)
        scope.is_a?(Array) ? scope.last : scope
      end

      unless defined? SplitGroups
        def handle_command(cmd, sock)
          case cmd
          when /^POP/
            # like the original, but no `.to_s`
            if obj = @queue.shift
              data = Marshal.dump(obj)
              sock.write(data)
            end
          else
            super
          end
        end
      end
    end

    # This replaces @queue
    module Queue
      class << self
        extend Forwardable

        def_delegator :split_queue, :<<

        attr_accessor :group_loaded

        # While it is as lazy as possible, you can request that it load
        # a certain number up front. This is useful in conjunction with
        # preferred tags and a custom order_files method. That way you
        # can ensure certain files are preloaded before anything starts.
        attr_accessor :preload_count
        def preload!
          @preloaded = true
          (preload_count || 0).times { queue_up_lazy_group }
        end

        def index
          preload! unless @preloaded
          queue.index &Proc.new
        end

        def delete_at(index)
          queue_up_lazy_groups
          queue.delete_at(index)
        end

        def empty?
          queue.empty? && BackgroundLoaderProxy.empty? && split_queue.empty?
        end

        def shift
          preload! unless @preloaded
          return if empty?
          queue_up_lazy_groups
          puts "All groups assigned" if ENV["TEST_QUEUE_VERBOSE"] && queue.size == 1 && BackgroundLoaderProxy.empty?
          queue.shift || split_queue.shift
        end

        # Not necessarily accurate, since BackgroundLoaderProxy.count is
        # the number of files left to load (each of which could have
        # multiple top-level groups) -- but the whole point here is we
        # don't want want to load up :allthethings: :P
        def size
          queue.size + BackgroundLoaderProxy.count + split_queue.size
        end

       private

        def queue_up_lazy_groups
          # pull in any already background-loaded ones
          queue_up_lazy_group while BackgroundLoaderProxy.queue_size > 0
          # we might be faster than the background loader, so block
          # until we get one (or it's done)
          queue_up_lazy_group if queue.empty?
        end

        def queue_up_lazy_group
          group_info = BackgroundLoaderProxy.shift or return
          file, groups = group_info
          groups.each do |group|
            group_loaded.call group
          end
          key = [file, groups.first.name]
          queue << [:group, key]
        end

        def queue
          @queue ||= []
        end

        def split_queue
          @split_queue ||= []
        end
      end
    end

    # This replaces @suites, so that when a worker gets assigned the next
    # top-level group, it will auto-load it
    class GroupLoader
      class << self
        def loaded(group)
          group_lookup[group.to_s] = group
          loaded_groups << group
        end

        # TODO: perhaps have SplitGroups extend the @suites instance?
        if defined? SplitGroups
          def [](keys)
            keys = keys.dup
            file, key = keys.shift.pop
            group = group_lookup[key] || begin
              load file
              group_lookup[key]
            end
            group.reserve_items(keys)
            group
          end
        else
          def [](full_key)
            file, key = full_key
            group_lookup[key] || begin
              load file
              group_lookup[key]
            end
          end
        end

       private

        attr_accessor :current_file, :loaded_groups

        def group_lookup
          @group_lookup ||= {}
        end

        def load(file)
          self.loaded_groups = []
          self.current_file = file
          super file

          ::RSpec.world.add_descending_declaration_line_numbers_by_file loaded_groups
        end
      end
    end

    # On the master, hook into rspec's spec file loading mechanism so that
    # we don't actually load up anything synchronously, but instead do it
    # in a background process and then lazily populate the queue via IPC
    #
    # Basically:
    #
    # 1. The parent process tells the child process which files to load
    # 2. The child process loads each file and writes a bunch of metadata
    #    back to the parent
    # 3. The parent pulls any outstanding data its queue right before it
    #    shifts the next item
    #
    # So the lower bound on how quickly test-queue can finish is how long it
    # takes to load up your whole app and test-suite (step 3 can block on 2
    # if tests are completing in parallel faster than files are loading in
    # the background)
    class BackgroundLoaderProxy
      class << self
        attr_accessor :count
        attr_accessor :stats

        def shift
          return if empty?
          data = socket.gets or raise("unexpected EOF from background loader")
          if data == "EOF\n" # send an actual "EOF\n" string to signal we are done
            self.count = 0
            close
            return nil
          end
          self.count, file, groups = deserialize(data)
          (file_group_map[file] ||= Set.new) << groups.first.name
          [file, groups]
        end

        def file_group_map
          stats[:file_group_map] ||= {}
        end

        # We've started loading (count) and finished shifting (socket.nil?)
        def empty?
          count && socket.nil?
        end

        def order_files(files)
          files.sort_by do |file|
            base_cost = (file_group_map[file] || [])
              .map { |group| stats[group] }
              .compact
              .max || Float::INFINITY
            [
              # slowest top-level group in the file
              -base_cost,
              # or if we have no stats, file size as a proxy for time
              -File.size(file)
            ]
          end
        end

        def load_files(files)
          files = order_files(files)

          self.count = files.count
          parent, child = IO.pipe
          # Start loading all files within a child process so we don't block the master
          #  TODO: fork multiple background loaders, divvy up the files
          fork do
            parent.close
            BackgroundLoader.load_all files, stats, BufferedWriter.new(child)
            exit! 0
          end
          child.close
          self.socket = EagerReader.new(parent)
        end

        # how many items can we read before blocking?
        def queue_size
          socket && socket.queue_size || 0
        end

       private

        attr_accessor :socket

        def close
          socket.close
          self.socket = nil
        end

        def deserialize(str)
          ::Marshal.load(::Base64.decode64(str))
        end
      end
    end

    class BackgroundLoader
      class << self
        attr_accessor :count
        attr_accessor :stats

        def load_all(files, stats, socket)
          self.stats = stats
          self.socket = socket
          self.count = files.size
          files.each do |file|
            load_file file
          end
          socket.puts "EOF"
          socket.close
          puts "All files loaded" if ENV["TEST_QUEUE_VERBOSE"]
        end

        def load_file(file)
          self.loaded_groups = []
          self.current_file = file
          self.count -= 1
          load file

          ::RSpec.world.add_descending_declaration_line_numbers_by_file loaded_groups

          loaded_groups.each do |group|
            queue_up_group group
          end
        end

        def queue_up_group(group)
          group_info = group.descendants.map { |g|
            GroupQueue.for(g, stats)
          }
          return if group_info.all?(&:empty?)

          socket.puts serialize([count, current_file, group_info])
        end

        # As each top-level ExampleGroup is loaded, send all its (and its
        # descendants') info back to the master process so that it can
        # populate queues 'n such
        def loaded(group)
          loaded_groups << group
        end

       private

        attr_accessor :loaded_groups, :current_file, :socket

        def serialize(obj)
          ::Base64.encode64(::Marshal.dump(obj)).gsub(/\n/, '')
        end
      end
    end

    # like the built-in Queue, but much less passive about running the
    # writer thread
    class HungryQueue
      extend Forwardable
      def_delegators :@queue, :empty?, :size

      def initialize
        @queue = []
        @mutex = Mutex.new
      end

      def <<(item)
        @mutex.synchronize { @queue << item }
      end

      def shift
        Thread.pass while empty? && !closed?
        @mutex.synchronize { @queue.shift }
      end

      def close
        @closed = true
      end

      def closed?
        @closed
      end
    end

    # This wraps an IO object and proxies writes through a queue, sending
    # them to the real IO in a separate thread. This way we can keep
    # writing to it indefinitely, even if it's not being read on the other
    # end and the real buffer fills up; we want the background loader to
    # load files as fast as it can, even if the master isn't ready to start
    # de-queuing things
    class BufferedWriter
      def initialize(socket)
        @socket = socket
        @queue = HungryQueue.new
        start_writing
      end

      def puts(s)
        @queue << s
      end

      def close
        @queue.close
        @writer.join
      end

     private

      def start_writing
        @writer = ::Thread.new do
          while item = @queue.shift
            @socket.puts item
          end
          @socket.close
        end
      end
    end

    # This wraps an IO object and eagerly reads into a ruby buffer. This
    # makes sure the real buffer is drained so that writes can keep
    # happening on the other end; we want the background loader to load
    # files as fast as it can, even if the master isn't ready to start
    # de-queuing things
    class EagerReader
      def initialize(socket)
        @socket = socket
        @queue = HungryQueue.new
        eagerly_read
      end

      def gets
        @queue.shift
      end

      def close
        @reader.join
      end

      def queue_size
        @queue.size
      end

     private

      def eagerly_read
        @reader = ::Thread.new do
          while line = @socket.gets
            @queue << line
          end
          @queue.close
          @socket.close
        end
      end
    end

    module Extensions
      module Configuration
        if ENV["TEST_QUEUE_RELAY"]
          # Make up-front loading a no-op on workers, so that they can start up
          # right away (we'll load on demand when we pop)
          def load(file)
            BackgroundLoaderProxy.count ||= 0
            BackgroundLoaderProxy.count += 1
          end
        else
          # Defer up-front loading on the master so we can assign out the queue
          # while we're still loading files
          def load(file)
          end

          def load_spec_files
            super
            BackgroundLoaderProxy.load_files loaded_spec_files
          end
        end
      end

      module World
        # not a great number, but really just needed so the reporter
        # doesn't spit out `No examples found.`
        def example_count(groups = example_groups)
          BackgroundLoaderProxy.count
        end

        # RSpec < 3.5
        def register(group)
          super
          LazyGroups.loader.loaded(group)
          group
        end

        # RSpec >= 3.5
        def record(group)
          super
          LazyGroups.loader.loaded(group)
          group
        end

        # `descending_declaration_line_numbers_by_file` assumes all groups are
        # registered up front, and then caches everything. Since we lazily load
        # groups, we need to add their info to this hash after the fact.
        def add_descending_declaration_line_numbers_by_file(example_groups)
          # if not set, it will auto-populate for existing groups when
          # requested, so we can avoid redundant work here
          return unless instance_variable_defined? :@descending_declaration_line_numbers_by_file

          declaration_locations =  ::RSpec::Core::FlatMap.flat_map(example_groups, &:declaration_locations)
          line_nums_by_file = ::Hash.new { |h, k| h[k] = [] }

          declaration_locations.each do |file_name, line_number|
            line_nums_by_file[file_name] << line_number
          end

          line_nums_by_file.each do |file, list|
            descending_declaration_line_numbers_by_file[file] = list
            list.sort!
            list.reverse!
          end
        end
      end

      module ExampleGroups
        # Normally `describe "some foo"` => `SomeFoo`, unless there are multiple
        # describes with the same description, in which case they get increasing
        # `_N` suffixes. But that depends entirely on require order, and we
        # don't necessarily even require everything on each worker anymore. So
        # we need a deterministic way of naming these classes. This should work
        # and be fast, unless you are doing something really really weird.
        def disambiguate(name, const_scope)
          return super if const_scope != self
          root = ::RSpec::Core::RubyProject.root + "/"
          pattern = ::Regexp.new("\\A" + Regexp.escape(root))
          location = caller.grep(pattern).join(" ").gsub(root, "")
          checksum = ::Zlib.crc32(location).to_s(16)
          "#{name}_#{checksum}"
        end
      end
    end
  end

  prepend LazyGroups::Runner
  ::RSpec::Core::Configuration.prepend LazyGroups::Extensions::Configuration
  ::RSpec::Core::World.prepend LazyGroups::Extensions::World
  ::RSpec::ExampleGroups.singleton_class.prepend LazyGroups::Extensions::ExampleGroups
end
