require 'test_queue/runner'
require 'rspec/core'


module TestQueue
  class Runner
    class RSpec < Runner
      def initialize(options = {})
        @rspec = ::RSpec::Core::QueueRunner.new
        queue = @rspec.example_groups
        queue = queue.sort_by{ |s| -(stats[s.to_s] || 0) } unless options[:no_sort]
        super(queue)
      end


      def run_worker(iterator)
        @rspec.run_each(iterator).to_i
      end

      # since groups can span runners, we save off the old stats,
      # figure out our new stats across all runners, and merge
      # into the old stats
      def summarize_internal
        @previous_stats = stats
        @stats = {}
        super
      end

      def save_stats
        @stats = @previous_stats.merge(stats)
        super
      end

      def summarize_worker(worker)
        worker.stats.each do |s, val|
          # take the cost of the slowest one across all runners;
          # when splitting a group, the cost is the time of the slowest
          # sub-group or example, not the group as a whole
          stats[s] = [val, stats[s]].compact.max
        end

        worker.summary  = worker.lines.grep(/ examples?, /).first
        worker.failure_output = worker.output[/^Failures:\n\n(.*)\n^Finished/m, 1]
      end
    end
  end
end

case ::RSpec::Core::Version::STRING.to_i
when 2
  require_relative 'rspec2'
when 3
  require_relative 'rspec3'

  # you can use split groups, or lazy groups, or both, but they need to load in this order
  require_relative 'rspec/split_groups' if ["1", "true"].include?(ENV.fetch("TEST_QUEUE_SPLIT_GROUPS", "0").downcase)
  require_relative 'rspec/lazy_groups' if ["1", "true"].include?(ENV.fetch("TEST_QUEUE_LAZY_GROUPS", "0").downcase)
else
  fail 'requires rspec version 2 or 3'
end
