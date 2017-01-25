class TestQueue::Runner::RSpec
  module Runner
    def group_queues
      @group_queues ||= {}
    end

    def add_group_queue(queue)
      group_queues[queue.name] = queue
    end
  end

  GroupQueue = Struct.new(:name, :queue, :tags) do
    def num_examples
      queue.count { |type, name| type == :example }
    end

    def empty?
      queue.empty?
    end

    TRACKED_METADATA = %i[no_split]

    def self.for(group, stats)
      items = group.filtered_items_hash[:example].keys.map { |name| [:example, name] } +
              group.filtered_items_hash[:group].keys.map { |name| [:group, name] }
      items.sort_by! { |type, item| -(stats[item] || Float::INFINITY) } if defined? SplitGroups

      new group.to_s,
          items,
          Hash[TRACKED_METADATA.map { |key| [key, group.metadata[key]] }]
    end

    # uniquely identify this group or example
    def self.stat_key_for(thing)
      if thing.is_a?(::RSpec::Core::Example)
        if thing.metadata[:description].to_s.empty?
          thing.id
        else
          thing.full_description
        end
      else
        thing.to_s
      end
    end
  end

  class GroupResolver
    extend Forwardable
    attr_reader :lookup
    def_delegator :lookup, :[]

    def initialize(groups = [])
      @lookup = {}
      groups.each do |group|
        lookup[group.to_s] = group
      end
    end

    def fetch(key)
      lookup[key]
    end
  end

  module Extensions
    module ExampleGroup
      def filtered_items_hash
        @filtered_items_hash ||= {
          example: ::Hash[
            ::RSpec.world.filtered_examples[self].map { |example|
              [GroupQueue.stat_key_for(example), example]
            }
          ],
          group: ::Hash[
            children.select { |group| group.filtered_items_hash.any? }
                    .map { |group| [GroupQueue.stat_key_for(group), group] }
          ]
        }
      end
    end
  end

  prepend Runner
  ::RSpec::Core::ExampleGroup.singleton_class.prepend Extensions::ExampleGroup
end
