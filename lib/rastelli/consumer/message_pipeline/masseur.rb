module Rastelli
	class Consumer
		class MessagePipeline
			class Masseur

				def initialize(decoder, message_queue)
					@decoder = decoder
					@message_queue = message_queue
				end

				def process(messages)
					decoded = messages.map { |m| @decoder.decode(m) }
					decoded.each { |d| @message_queue.put(d) }
				end

			end
		end
	end
end	
