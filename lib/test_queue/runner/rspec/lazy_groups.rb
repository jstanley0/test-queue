require_relative "group_queue"
require 'zlib'
require 'socket'
require 'base64'

class TestQueue::Runner::RSpec
  module LazyGroups
    class << self
      attr_accessor :loader
    end

    module Runner
      def initialize(*)
        BackgroundLoaderProxy.stats = stats
        super no_sort: true
      end

      def start_master
        super
        BackgroundLoaderProxy.load_files @server unless relay?
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

      def file_group_map
        (ENV["TEST_QUEUE_REPLACE_STATS"] ? new_stats : stats)[:file_group_map] ||= {}
      end

      def queue_up_lazy_group(loader_num, files_remaining, file, groups)
        BackgroundLoaderProxy.counts[loader_num] = files_remaining
        (file_group_map[file] ||= Set.new) << groups.first.name

        groups.each do |group|
          add_group_queue group
        end
        key = [file, groups.first.name]
        @queue.push [:group, key]
      end

      if defined? SplitGroups
        def handle_command(cmd, sock)
          case cmd
          when /^NEW SUITE (\d+)/
            queue_up_lazy_group(*Marshal.load(sock.read($1.to_i)))
          else
            super
          end
        end
      else
        def handle_command(cmd, sock)
          case cmd
          when /^NEW SUITE (\d+)/
            queue_up_lazy_group(*Marshal.load(sock.read($1.to_i)))
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

    module Iterator
      def query(payload)
        while true
          result = super
          break unless result == :wait # we might have caught up to the background loader
          sleep 0.1
        end
        result
      end
    end

    # This replaces @queue
    module Queue
      class << self
        extend Forwardable

        def_delegator :queue, :push
        def_delegator :split_queue, :<<

        def empty?
          size == 0
        end

        def shift
          return if empty?
          queue.shift || split_queue.shift || (BackgroundLoaderProxy.empty? ? nil : :wait)
        end

        # Not necessarily accurate, since BackgroundLoaderProxy.count is
        # the number of files left to load (each of which could have
        # multiple top-level groups) -- but the whole point here is we
        # don't want want to load up :allthethings: :P
        def size
          queue.size + BackgroundLoaderProxy.count + split_queue.size
        end

       private

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
              group_lookup[key] or raise("expected #{file.inspect} to define #{key.inspect}, it did not")
            end
            group.reserve_items(keys)
            group
          end
        else
          def [](full_key)
            file, key = full_key
            group_lookup[key] || begin
              load file
              group_lookup[key] or raise("expected #{file.inspect} to define #{key.inspect}, it did not")
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
    # If workers are pulling stuff off the queue faster than the background
    # loaders can run, the master will tell them to wait.
    class BackgroundLoaderProxy
      NUM_LOADERS = 2

      class << self
        attr_accessor :counts
        attr_accessor :stats

        def empty?
          count == 0
        end

        def count
          counts ? counts.inject(&:+) : 1
        end

        def file_group_map
          stats[:file_group_map] ||= {}
        end

        def spec_files
          @spec_files ||= []
        end

        def order_files(files)
          file_group_map = stats[:file_group_map] || {}
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

        def load_files(server)
          files = order_files(spec_files)
          chunks = NUM_LOADERS.times.map { [] }
          files.each_with_index do |file, i|
            chunks[i % NUM_LOADERS] << file
          end

          self.counts = chunks.map(&:size)

          chunks.each_with_index do |list, num|
            fork do
              BackgroundLoader.new num, list, stats, server
              exit! 0
            end
          end
        end
      end
    end

    class BackgroundLoader
      attr_accessor :num
      attr_accessor :count
      attr_accessor :stats
      attr_accessor :server

      def initialize(num, files, stats, server)
        LazyGroups.loader = self

        self.num = num
        self.stats = stats
        self.server = server
        self.count = files.size
        files.each do |file|
          load_file file
        end

        puts "All files loaded (#{num + 1}/#{BackgroundLoaderProxy::NUM_LOADERS})" if ENV["TEST_QUEUE_VERBOSE"]
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

        payload = Marshal.dump([num, count, current_file, group_info])
        server.connect_address.connect do |sock|
          sock.puts "NEW SUITE #{payload.size}"
          sock.write payload
        end
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

    module Extensions
      module Configuration
        # Defer up-front loading on the master so we can assign out the queue
        # while we're still loading files
        def load(file)
          BackgroundLoaderProxy.spec_files << file
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
  ::TestQueue::Iterator.prepend LazyGroups::Iterator
  ::RSpec::Core::Configuration.prepend LazyGroups::Extensions::Configuration
  ::RSpec::Core::World.prepend LazyGroups::Extensions::World
  ::RSpec::ExampleGroups.singleton_class.prepend LazyGroups::Extensions::ExampleGroups
end
