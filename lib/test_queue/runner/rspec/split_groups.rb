module RSpec::Core
  class ExampleIterator
    include Enumerable

    def initialize(group, iterator, examples)
      @iterator = iterator
      @group = group
      @examples = Hash[examples.map { |example| [example.full_description, example ]}]
    end

    def each
      while true
        key = @iterator.pop_example(@group.to_s) or break
        yield @examples[key]
      end
      self
    end

    def ordered
      self
    end

    def size
      @examples.size
    end
  end

  class NoOpOrderer
    def order(items)
      items
    end
  end

  class ExampleGroup
    # rspec uses this to determine if before/after :all hooks should run.
    # if there are still examples in the queue, return any that we might
    # run... we have no way of knowing yet which ones we'll actually run,
    # since other workers can pull stuff off the queue
    #
    # NOTE: there's a race condition where the last example could be
    # claimed by someone else after we call this, meaning we might do a
    # little bit of unnecessary work, but ¯\_(ツ)_/¯
    def self.descendant_filtered_examples
      @descendant_filtered_examples ||= begin
        if TestQueue.iterator.has_descendant_examples_in_queue?(self.to_s)
          filtered_examples = RSpec.world.filtered_examples[self]
          filtered_examples + FlatMap.flat_map(children, &:descendant_filtered_examples)
        else
          []
        end
      end
    end

    # make sure rspec gets the examples from the iterator...
    #
    # rspec2 does `filtered_examples.ordered.map`
    # rspec3 does `ordering_strategy.order(filtered_examples).map`

    def self.filtered_examples
      ExampleIterator.new(self, TestQueue.iterator, RSpec.world.filtered_examples[self])
    end

    # don't try to sort, since it's an iterator
    def self.ordering_strategy
      NoOpOrderer.new
    end
  end
end

