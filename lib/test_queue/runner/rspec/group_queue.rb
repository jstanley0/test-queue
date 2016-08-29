class TestQueue::Runner::RSpec
  module Runner
    def group_queues
      @group_queues ||= {}
    end

    def add_group_queue(queue)
      group_queues[queue.name] = queue
    end
  end

  GroupQueue = Struct.new(:name, :examples, :groups, :tags) do
    def empty?
      examples.empty? && groups.empty?
    end

    TRACKED_METADATA = %i[no_split]

    def self.for(group)
      new group.to_s,
          group.filtered_items_hash[:example].keys,
          group.filtered_items_hash[:group].keys,
          Hash[TRACKED_METADATA.map { |key| [key, group.metadata[key]] }]
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
              [example.full_description, example]
            }
          ],
          group: ::Hash[
            children.select { |group| group.filtered_items_hash.any? }
                    .map { |group| [group.to_s, group] }
          ]
        }
      end
    end
  end

  prepend Runner
  ::RSpec::Core::ExampleGroup.singleton_class.prepend Extensions::ExampleGroup
end
