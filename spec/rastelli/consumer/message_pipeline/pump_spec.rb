require 'spec_helper'

module Rastelli
  class Consumer
    class MessagePipeline

      FakeMessage = Struct.new(:offset)

      describe Pump do

        let(:registry) { mock(Registry).as_null_object }

        let(:source) do 
          mock('Source').as_null_object.tap do |s|
            s.stub(:fetch) do |streams_offsets|
              streams_offsets.inject(Hash.new) do |memo, (stream, offset)|
                memo[stream] = [FakeMessage.new(offset + 1)]
                memo
              end
            end
          end
        end

        let(:masseur) { mock('Masseur').as_null_object }
        let(:pump) { Pump.new(registry, source, masseur) }

        let(:streams_and_offsets) do
          {
            '0' => 0,
            '1' => 1,
            '2' => 2
          }
        end

        describe '#work' do

          before(:each) do
            registry.stub(:streams_and_offsets).and_return(streams_and_offsets)
          end

          it 'fetches streams and offsets' do
            registry.should_receive(:streams_and_offsets)

            pump.work
          end

          it 'fetches messages' do
            source.should_receive(:fetch)

            pump.work
          end
          
          it 'pushes messages further' do
            masseur.should_receive(:process)

            pump.work
          end
          
          it 'updates registry with new offsets' do
            streams_and_offsets.each do |stream, offset|
              registry.should_receive(:update_offset).with(stream, offset + 1)
            end

            pump.work
          end

        end
      end
    end
  end
end
