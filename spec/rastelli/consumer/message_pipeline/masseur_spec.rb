require 'spec_helper'

module Rastelli
	class Consumer
		class MessagePipeline

			describe Masseur do

				let(:decoder) do 
					mock('Decoder').tap do |d|
						d.stub(:decode).and_return { |m| m }
					end
				end

				let(:message_queue) { Concurrency::ArrayBlockingQueue.new(10) }
				let(:masseur) { Masseur.new(decoder, message_queue) }

				let(:messages) { (1..10).to_a }

				describe '#process' do

					it 'decodes and pushes messages to queue' do
						decoder.should_receive(:decode).exactly(messages.length).times
						message_queue.should_receive(:put).exactly(messages.length).times

						masseur.process(messages)
					end
				end

			end
		end
	end
end
