module Rastelli
	class Consumer
		class Registry

			EARLIEST_OFFSET = -2 # Placeholder until heller is updated

			def initialize
				@registered_streams = Concurrency::ConcurrentHashMap.new
				@suspended_streams 	= Concurrency::ConcurrentHashMap.new
			end

			def register(stream)
				@registered_streams[stream] ||= EARLIEST_OFFSET # Heller::Consumer::EARLIEST_OFFSET
			end

			def registered?(stream)
				@registered_streams.contains_key?(stream)
			end

			def unregister(stream)
				@registered_streams.remove(stream)
			end

			def suspend(stream)
				if registered?(stream)
					@suspended_streams[stream] = stream 
					unregister(stream)
				end
			end

			def forget(stream)
				unregister(stream) || @suspended_streams.remove(stream)
			end

			def registered_streams
				@registered_streams.keys.to_a
			end

			def suspended_streams
				@suspended_streams.keys.to_a
			end

			def update_offset(stream, offset)
				if registered?(stream)
					@registered_streams[stream] = offset
				end
			end

			def offset_for(stream)
				@registered_streams[stream]
			end

			def streams_and_offsets
				@registered_streams
			end
		end
	end
end
