module Rastelli
  class Consumer
    class MessagePipeline
      class Pump

        def initialize(registry, source, masseur)
          @registry = registry
          @source = source
          @masseur = masseur
        end

        def work
          streams_and_offsets = @registry.streams_and_offsets
          fetch_response = @source.fetch(streams_and_offsets)

          @masseur.process(fetch_response)

          fetch_response.each do |stream, messages|
            @registry.update_offset(stream, messages.last.offset)
          end
        end

      end
    end
  end
end
